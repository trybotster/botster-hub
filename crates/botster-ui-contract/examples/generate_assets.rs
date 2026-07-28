use std::fs;
use std::path::PathBuf;

fn main() {
    let package = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../packages/ui-contract");
    let assets = [
        ("index.d.ts", botster_ui_contract::typescript_declarations()),
        (
            "schema.json",
            format!(
                "{}\n",
                serde_json::to_string_pretty(&botster_ui_contract::json_schema())
                    .expect("serialize JSON schema")
            ),
        ),
        (
            "conformance-fixtures.json",
            format!(
                "{}\n",
                serde_json::to_string_pretty(&botster_ui_contract::conformance_fixtures_json())
                    .expect("serialize conformance fixtures")
            ),
        ),
    ];

    if std::env::args().any(|argument| argument == "--check") {
        for (name, expected) in assets {
            let actual = fs::read_to_string(package.join(name))
                .unwrap_or_else(|error| panic!("read generated {name}: {error}"));
            assert_eq!(actual, expected, "generated {name} is stale");
        }
        return;
    }

    fs::create_dir_all(&package).expect("create ui-contract package directory");
    for (name, contents) in assets {
        fs::write(package.join(name), contents)
            .unwrap_or_else(|error| panic!("write generated {name}: {error}"));
    }
}
