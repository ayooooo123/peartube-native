//! The zero-install Smart TV page (`src/cast-ui.js`), kept as one static document.

use serde_json::json;

const RECEIVER_PAGE: &str = include_str!("ui/receiver.html");

/// `receiverPage({ mode, defaultName })`. The page's single embedded script literal is `__BOOT__`
/// in the asset; the boot object is substituted here so the HTML itself stays byte-identical.
pub fn receiver_page(mode: &str, default_name: &str) -> String {
    let boot = json!({ "mode": mode, "defaultName": default_name }).to_string();
    RECEIVER_PAGE.replace("__BOOT__", &boot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boot_object_lands_in_the_page() {
        let page = receiver_page("hub", "Smart TV Browser");
        assert!(page.contains(r#"const BOOT = {"mode":"hub","defaultName":"Smart TV Browser"};"#));
        assert!(!page.contains("__BOOT__"));
    }
}
