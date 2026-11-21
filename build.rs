fn main() {
    hyperschema::build(
        "spec/hyperschema/schema.json",
        "peartube_wire.rs",
        &hyperschema::Options::new(),
    )
    .expect("compile PearTube wire schema");
}
