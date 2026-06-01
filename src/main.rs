fn main() {
    let summary = botster_hub::architecture_summary();
    println!(
        "botster-hub scaffold: {} roles, {} provider capability contracts",
        summary.responsibilities().len(),
        summary.provider_capabilities().len()
    );
}
