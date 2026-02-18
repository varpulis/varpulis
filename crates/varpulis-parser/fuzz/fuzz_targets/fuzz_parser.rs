#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Limit input size to prevent pathological stack usage in pest's
    // recursive descent parser. Real VPL programs are well under 64KB.
    if data.len() > 65_536 {
        return;
    }
    if let Ok(source) = std::str::from_utf8(data) {
        // The parser must never panic on any input
        let _ = varpulis_parser::parse(source);
    }
});
