# The shared part of the Botster gate job: everything of ci/remote/job.sh except its settings block. job.sh sets the
# settings and then sources this file; it is not run on its own.
#
# botster-contracts owns this file. botster-core keeps a byte copy of it at its pinned botster-contracts revision, and a
# botster-core check fails on any byte difference (steward ruling, 2026-10-11). Change this file here first, then copy it
# into botster-core at the next pin move. A line that only one repository needs goes into the settings of its job.sh.
: "${project:?job-common.sh: source it from ci/remote/job.sh, after the settings}"

dir=$(realpath -- "${1:?job.sh: no snapshot directory}")
name= secrets= watcher=
# The GitHub token for private dependencies: the pool's secret BOTSTER_GITHUB_TOKEN (Nomad Variable pool-secrets/botster,
# compute-pool DESIGN.md "Pool secrets") when the pool gives one, else the host's GitHub CLI login. Kept in a shell
# variable, not exported, so no later process inherits it.
export -n github_token 2>/dev/null; github_token=${BOTSTER_GITHUB_TOKEN:-}
unset BOTSTER_GITHUB_TOKEN

# Deletes <child> only when its real path is one directory directly inside <parent>.
remove_child() {
  local parent child
  [ -e "$2" ] || return 0
  parent=$(realpath -- "$1") child=$(realpath -- "$2") || return 0
  [[ $child == "$parent"/* && ${child#"$parent"/} != */* ]] || return 0
  rm -rf -- "$child"
}

cleanup() {
  local status=$?
  # Every step runs, whatever the one before returned (an already-ended watcher, a container already gone).
  set +e
  # testq cancel TERMs the job's process group and testq-launch forwards a second TERM; ignore both so cleanup finishes.
  trap - EXIT
  trap '' INT TERM HUP
  [ -n "$watcher" ] && kill "$watcher" 2>/dev/null
  if [ -n "$name" ]; then
    docker ps --all --quiet --filter "label=testq.job=$name" | xargs --no-run-if-empty docker rm --force >/dev/null 2>&1 || true
  fi
  [ -n "$secrets" ] && remove_child "${TMPDIR:-/tmp}" "$secrets"
  [[ $dir =~ /testq/projects/[^/]+/jobs/[^/]+$ ]] && remove_child "${dir%/*}" "$dir"
  exit "$status"
}
trap cleanup EXIT
# Long docker runs are waited on in the background so a signal interrupts them. Each handler ignores further signals
# before it exits, so a second signal cannot cut cleanup short (HyperFlex test/docker/job.sh has the full reasoning).
on_signal() { trap '' INT TERM HUP; exit "$1"; }
trap 'on_signal 129' HUP
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

cpus=${2:?job.sh: no CPU count}
shift 2
run=$(basename "$dir") branch= base= lease=
while (( $# )); do
  case $1 in
    --run=*) run=${1#--run=} ;;
    --branch=*) branch=${1#--branch=} ;;
    --base=*) base=${1#--base=} ;;
    --lease=*) lease=${1#--lease=} ;;
    --) shift; break ;;
    *) echo "job.sh: unknown option '$1' (the command follows --)" >&2; exit 64 ;;
  esac
  shift
done
reject() { echo "job.sh: $1" >&2; exit 64; }
segment='^[A-Za-z0-9][A-Za-z0-9_.-]*$'
[[ $run =~ $segment ]] || reject "invalid --run name"
[[ $cpus =~ ^[1-9][0-9]*$ ]] || reject "invalid CPU count"
(( $# )) || reject "no command given"
[ -z "$base" ] || [[ $base =~ ^[0-9a-f]{40}$ ]] || reject "--base must be a full commit sha"
[ -z "$lease" ] || [[ $lease =~ ^$HOME/testq/projects/botster-locks/leases/[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] \
  || reject "invalid --lease file"

# The client's lease: a free lock means the client is gone (Ctrl-C it could not report, a lost connection, a killed shell).
# The holder deletes the file when it exits; flock would create it again, so a missing file also means "gone".
client_gone() { [ ! -e "$lease" ] || flock -n "$lease" true; }
if [ -n "$lease" ]; then
  if client_gone; then
    echo "job.sh: the client is gone; the job does not start" >&2
    exit 125
  fi
  # timer: deadline — polls the lease every 5 s; flock has no wait-for-release-by-another-process primitive here.
  ( while sleep 5; do
      if client_gone; then
        echo "job.sh: the client is gone (no heartbeat); stopping the job" >&2
        kill -TERM $$
        exit 0
      fi
    done ) &
  watcher=$!
fi

# A branch name becomes a volume name: lower case, [a-z0-9_.-], with a hash of the full name against collisions.
if [ -z "$branch" ]; then branch=$(git -C "$dir" rev-parse --abbrev-ref HEAD); fi
slug=$(printf %s "$branch" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9_.\n-' '-' | cut -c1-40)
slug=$slug-$(printf %s "$branch" | sha1sum | cut -c1-8)

projdir=$(dirname "$(dirname "$dir")")
artifacts=$projdir/artifacts
stamps=$projdir/targets
name=testq-$project-$(basename "$dir" | sha1sum | cut -c1-10)
hash=$(cd "$dir" && { cat ci/remote/Dockerfile "${image_inputs[@]}"; image_args; } | sha256sum | cut -c1-12)
image=$project-gate:$hash
toolchain=$(sed -n 's/^channel *= *"\(.*\)"/\1/p' "$dir/rust-toolchain.toml")
# The containers' user: the owner of the job's files, so git accepts /work and the steps can write it. Rootful Docker:
# this user's uid and gid. Rootless Docker (the compute pool's job user): container uid 0 is this user on the host, and
# any other container uid is a subordinate uid that owns nothing here (git refuses /work: "dubious ownership"). There
# the containers run as uid 0 with every capability dropped and no new privileges, so the permission bits still apply
# to them as to an ordinary owner (no CAP_DAC_OVERRIDE). The tests' root checks (geteuid() == 0) still see root and
# skip their permission-denied cases; the Mac tier and rootful Linux hosts run those cases.
# The volumes of a rootless daemon have their own names ($project-rootless-*): volumes that an earlier container user
# owns are never changed (a job of that user may still use them); they stay idle, and the compute pool removes idle job
# volumes (pool-volumes). The first rootless job of a project starts with empty caches.
security=$(docker info --format '{{range .SecurityOptions}}{{println .}}{{end}}') \
  || { echo "job.sh: docker info failed; the containers' user cannot be chosen" >&2; exit 3; }
if grep -qx 'name=rootless' <<< "$security"; then
  ctr_uid=0 ctr_gid=0 ctr_user=(--user 0:0 --cap-drop ALL --security-opt no-new-privileges) vol=$project-rootless
else
  ctr_uid=$(id -u) ctr_gid=$(id -g) ctr_user=(--user "$ctr_uid:$ctr_gid") vol=$project
fi
cargo_volume=$vol-cargo
target_volume=$vol-target-$slug
npm_volume=$vol-npm

# Priority jobs (CI) get more CPU weight when the host is busy. Memory: rustc and the linker need about 2 GB a CPU, plus the
# gate container's tmpfs /tmp (tmp_gb).
tmp_gb=4
shares=$(( ${TESTQ_PRIORITY:-0} ? 2048 : 1024 ))
limits=(--cpus "$cpus" --cpu-shares "$shares" --memory "$(( cpus * 2 + 4 + tmp_gb ))g")

# --init: tini is PID 1 and reaps orphans, as launchd does on the Mac. Without it, a test's reparented child stays a zombie
# and the leftover-process check fails.
# Network: a container on Docker's default bridge adds a veth to the host at its start and removes it at its end, and the
# host's browsers abort their requests in flight at each such network change (net::ERR_NETWORK_CHANGED in other projects'
# suites on the pool hosts). `--network none` adds no veth. So `container` runs with no network, and only a step that
# downloads runs through `online`.
run_container() {
  local network=$1
  shift
  docker run --rm --init --network "$network" --label "testq.job=$name" "${limits[@]}" "${ctr_user[@]}" \
    -e HOME=/tmp "$@" &
  wait $!
}
container() { run_container none "$@"; }
online() { run_container bridge "$@"; }
# online_stdin: online, with this shell's stdin passed to the container (docker run -i). A background command's stdin is
# /dev/null unless it is redirected, so run_container's docker gets it explicitly.
online_stdin() {
  docker run --rm --init -i --network bridge --label "testq.job=$name" "${limits[@]}" "${ctr_user[@]}" \
    -e HOME=/tmp "$@" <&0 &
  wait $!
}

echo "gate job: $project $(git -C "$dir" rev-parse HEAD) on $(hostname), $cpus CPUs, image $image, target $target_volume"

mkdir -p "$artifacts" "$stamps"
find "$artifacts" -mindepth 1 -maxdepth 1 -type d -mtime +7 -exec rm -rf {} +
# The image lock covers this project's image build.
exec {image_lock}>"$projdir/image.lock"
flock "$image_lock"
if ! docker image inspect "$image" >/dev/null 2>&1; then
  echo "Building $image (rust $toolchain)..."
  build_args=(--build-arg "RUST_TOOLCHAIN=$toolchain" --build-arg "BUILD_JOBS=$cpus")
  while IFS= read -r arg; do [ -n "$arg" ] && build_args+=(--build-arg "$arg"); done < <(image_args)
  build_log=$projdir/image-build.log
  docker build --progress=plain "${build_args[@]}" -t "$image" "$dir/ci/remote" >"$build_log" 2>&1 &
  wait $! || { tail -n 40 "$build_log"; echo "job.sh: the image build failed; the full log is $build_log on the host" >&2; exit 1; }
  # Drop superseded images; Docker refuses to remove one that a running job still uses.
  docker image ls "$project-gate" --format '{{.Repository}}:{{.Tag}}' | grep -vxF "$image" \
    | xargs --no-run-if-empty docker image rm >/dev/null 2>&1 || true
fi
exec {image_lock}>&-

# Disk. The host is shared, and a target volume holds about 10 GB, so target volumes are bounded:
#   - at most $max_targets per project; the least recently used go first, the base branch's (main, v1) last;
#   - below $floor_gb GB free, least recently used Botster target volumes of either project go until the floor is met;
#   - below $fail_gb GB free after that, the gate fails at once instead of filling the disk mid-run.
# A job holds a shared "in use" lock on its target volume (botster-locks/use-<volume>.lock) from before it releases the
# volumes lock until it exits, through every gap between its containers; eviction needs that lock exclusively. A volume
# whose stamp is newer than $fresh_min minutes is not evicted either. One Botster-wide lock covers the stamps, the
# pruning, the in-use locks and the volume initialization of both projects.
max_targets=8 floor_gb=60 fail_gb=40 fresh_min=120
botster_projects=$(dirname "$projdir")
locks_dir=$botster_projects/botster-locks
# Free space in bytes (df -BG rounds up), and the comparison with a threshold in GiB. The target volumes live in Docker's
# data root, which can be on another filesystem than $projdir (the pool's Linux nodes keep it on /pool), so the floor and
# the fail check measure the filesystem of the data root. Every measurement that decides goes through `measure`, which
# runs in the job's shell: a data root that cannot be found, a failed df, or an answer that is not a number stops the gate
# (exit 75) before any container starts, because the checks cannot hold without it. A measurement in $(...) or in a
# condition would turn a failure into "not below" instead.
free_on() { df -B1 --output=avail -- "$1" | tail -n 1; }
docker_root=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null) || docker_root=
# Sets free_now to the free bytes of the data root, or ends the job with 75.
measure() {
  local answer=
  [ -z "$docker_root" ] || answer=$(free_on "$docker_root" 2>/dev/null) || answer=
  answer=${answer//[[:space:]]/}
  if ! [[ $answer =~ ^[0-9]+$ ]]; then
    echo "job.sh: the free space of Docker's data root (${docker_root:-not found by docker info}) cannot be measured (df: '${answer}'); the gate does not start." >&2
    exit 75
  fi
  free_now=$answer
}
below() { measure; (( free_now < $1 * 1024 * 1024 * 1024 )); }
measure
# Removes the volume of a stamp file, unless a job holds it (its in-use lock) or a container mounts it.
evict() {
  local volume use
  volume=$(basename -- "$1")
  exec {use}>"$locks_dir/use-$volume.lock"
  if ! flock -n -x "$use"; then exec {use}>&-; return 1; fi
  if ! docker volume rm "$volume" >/dev/null 2>&1; then exec {use}>&-; return 1; fi
  rm -f -- "$1" "$locks_dir/use-$volume.lock"
  exec {use}>&-
  echo "job.sh: removed the target volume $volume ($2)"
}
# Stamp files that may be evicted, least recently used first, the base branch's volumes last.
evictable() {
  find "$@" -maxdepth 0 -type f -mmin +"$fresh_min" -printf '%T@ %p\n' 2>/dev/null | sort -n | cut -d' ' -f2- \
    | awk '/-target-(main|v1)-[0-9a-f]+$/ { last = last $0 "\n"; next } { print } END { printf "%s", last }'
}
mkdir -p "$locks_dir"
exec {volumes_lock}>"$locks_dir/volumes.lock"
flock "$volumes_lock"
touch "$stamps/$target_volume"
# This job's in-use lock, held until it exits (the shell keeps the descriptor; containers and cleanup run inside it).
exec {in_use}>"$locks_dir/use-$target_volume.lock"
flock -s "$in_use"
while IFS= read -r stamp; do
  evict "$stamp" "no gate used it for 7 days" || true
done < <(find "$stamps" -mindepth 1 -maxdepth 1 -type f -name "$vol-target-*" -mtime +7)
count=$(find "$stamps" -mindepth 1 -maxdepth 1 -type f -name "$vol-target-*" | wc -l)
while IFS= read -r stamp; do
  (( count > max_targets )) || break
  evict "$stamp" "more than $max_targets target volumes of $project" && count=$(( count - 1 ))
done < <(evictable "$stamps"/"$vol"-target-*)
if below "$floor_gb"; then
  while IFS= read -r stamp; do
    below "$floor_gb" || break
    evict "$stamp" "Docker's data root is below $floor_gb GB free" || true
  done < <(evictable "$botster_projects"/botster-*/targets/botster-*-target-*)
fi
measure
free=$(( free_now / 1024 / 1024 / 1024 ))
if (( free_now < fail_gb * 1024 * 1024 * 1024 )); then
  echo "job.sh: Docker's data root $docker_root has $free GiB free, below $fail_gb GiB, after trimming Botster target volumes; the gate does not start. Free space on the host (other projects' volumes, docker build cache)." >&2
  exit 75
fi
# A new volume is given to the containers' user while it is empty; if that fails, the job removes the volume again.
# Then one container checks that every volume's top directory belongs to the containers' user, so a volume whose
# initialization failed (and whose removal failed too) is never used.
for volume in "$cargo_volume" "$target_volume" "$npm_volume" "${extra_volumes[@]/#/$vol-}"; do
  volume=${volume%%:*}
  if ! docker volume inspect "$volume" >/dev/null 2>&1; then
    docker volume create "$volume" >/dev/null
    docker run --rm --network none --label "testq.job=$name" -v "$volume:/v" "$image" chown "$ctr_uid:$ctr_gid" /v &
    if ! wait $!; then
      if docker volume rm "$volume" >/dev/null 2>&1; then
        echo "job.sh: cannot give the new volume $volume to the containers' user $ctr_uid:$ctr_gid; removed it" >&2
      else
        echo "job.sh: cannot give the new volume $volume to the containers' user $ctr_uid:$ctr_gid, and cannot remove it; remove it by hand (docker volume rm $volume)" >&2
      fi
      exit 3
    fi
  fi
done
owner_mounts=() owner_names=() n=0
for volume in "$cargo_volume" "$target_volume" "$npm_volume" "${extra_volumes[@]/#/$vol-}"; do
  owner_mounts+=(-v "${volume%%:*}:/v/$n") owner_names+=("${volume%%:*}") n=$(( n + 1 ))
done
docker run --rm --network none --label "testq.job=$name" "${owner_mounts[@]}" "$image" sh -c \
  'want=$1; shift; n=0; for name; do o=$(stat -c %u:%g "/v/$n") || exit 1
     [ "$o" = "$want" ] || { echo "job.sh: volume $name is owned by $o, not by $want" >&2; exit 1; }; n=$((n + 1)); done' \
  sh "$ctr_uid:$ctr_gid" "${owner_names[@]}" &
wait $! || { echo "job.sh: a volume of this job does not belong to the containers' user $ctr_uid:$ctr_gid (remove it with docker volume rm; it is a cache)" >&2; exit 3; }
exec {volumes_lock}>&-
project_free=$(free_on "$projdir" 2>/dev/null) || project_free=
project_free=${project_free//[[:space:]]/}
[[ $project_free =~ ^[0-9]+$ ]] && project_free="$(( project_free / 1024 / 1024 / 1024 )) GiB" || project_free="an unknown amount"
echo "gate job: $free GiB free on Docker's data root $docker_root (the target volumes); $project_free free on $projdir"

mounts=(-v "$dir:/work" -v "$target_volume:/work/target" -v "$cargo_volume:/cargo" -v "$npm_volume:/npm")
for volume in "${extra_volumes[@]}"; do mounts+=(-v "$vol-${volume%%:*}:${volume#*:}"); done
# After a fetch step, the gate runs with no network; a project with no fetch step downloads in the gate itself, so its gate
# runs online.
gate=online offline=()
gate_env=(-e TZ=UTC)
[ -n "$base" ] && gate_env+=(-e "BOTSTER_CI_BASE_REF=$base")
for variable in "${extra_env[@]}"; do gate_env+=(-e "$variable"); done

if (( private_git_deps )); then
  # Fetch every dependency with the GitHub token (above), in a container that runs no repo code but cargo's resolver.
  # The token reaches the container on its stdin and is written there to a 0600 file on a tmpfs that only the
  # container's user owns, read by a git credential helper for github.com only: never in a process argument, a
  # container's environment, the image, a volume, or a host file. (A host file does not work under rootless Docker: the
  # container's user, --user, maps to a subordinate uid that cannot read the job user's 0600 file.)
  if [ -n "$github_token" ]; then echo "job: private dependencies: GitHub token from the pool secret BOTSTER_GITHUB_TOKEN"
  else echo "job: private dependencies: GitHub token from gh auth token"; github_token=$(gh auth token); fi
  printf '%s' "$github_token" | online_stdin --name "$name-fetch" "${gate_env[@]}" "${mounts[@]}" -w /work \
    --tmpfs "/run/testq-secrets:rw,noexec,nosuid,nodev,size=64k,mode=0700,uid=$ctr_uid,gid=$ctr_gid" \
    -e GIT_TERMINAL_PROMPT=0 -e GIT_CONFIG_COUNT=1 \
    -e GIT_CONFIG_KEY_0=credential.https://github.com.helper \
    -e 'GIT_CONFIG_VALUE_0=!f() { cat >/dev/null; [ "$1" = get ] || exit 0; echo username=x-access-token; printf "password=%s\n" "$(cat /run/testq-secrets/github-token)"; }; f' \
    "$image" bash -c 'umask 077 && cat > /run/testq-secrets/github-token && exec bash ci/remote/fetch.sh </dev/null'
  github_token=
  gate=container
fi
# Public sources come in a container with network and without the token (ci/remote/fetch-public.sh; botster-core: the
# submodules and the Zig packages, which are built or unpacked from third-party code; botster-contracts: cargo's crates and
# the npm packages). In a project with no token fetch, it fetches every download of the gate.
if [ -f "$dir/ci/remote/fetch-public.sh" ]; then
  online --name "$name-fetch-public" "${gate_env[@]}" "${mounts[@]}" -w /work "$image" bash ci/remote/fetch-public.sh
  gate=container
fi
# The gate's cargo and npm read only the cargo volume and the npm cache (/npm) then.
if [ "$gate" = container ]; then offline=(-e CARGO_NET_OFFLINE=true -e npm_config_offline=true); fi

status=0
# /tmp is a tmpfs: the kernel throttles every writer of a container whose dirty page cache is at its limit, so the tests'
# temp roots (git repositories, small files) stalled for seconds while rustc or another test wrote gigabytes in the same
# container (measured: git init/add/commit 6 ms quiet, 566 ms median and 1.8 s max under such a writer, 13 ms on tmpfs).
"$gate" --name "$name" "${offline[@]}" "${gate_env[@]}" "${mounts[@]}" --tmpfs "/tmp:rw,exec,mode=1777,size=${tmp_gb}g" \
  -w /work "$image" "$@" || status=$?

# The failure reports ($artifact_paths of the settings): botster-contracts writes them in the tree, botster-core in the
# target volume, which only a container sees.
if (( status )); then
  remove_child "$artifacts" "$artifacts/$run"
  mkdir -p "$artifacts/$run"
  container --name "$name-artifacts" "${mounts[@]}" -v "$artifacts/$run:/out" "$image" \
    sh -c 'for p in "$@"; do
             [ -e "/work/$p" ] && cp -a "/work/$p" "/out/$(echo "$p" | tr / -)"; done; true' sh "${artifact_paths[@]}" || true
  if [ -n "$(ls -A "$artifacts/$run")" ]; then
    echo "Failure artifacts: $artifacts/$run (kept 7 days)"
  else
    rmdir "$artifacts/$run"
  fi
fi
exit "$status"
