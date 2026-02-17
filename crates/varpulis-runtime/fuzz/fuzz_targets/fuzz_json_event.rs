#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(input) = std::str::from_utf8(data) {
        // EventFileParser::parse_line must never panic on any input
        let _ = varpulis_runtime::event_file::EventFileParser::parse_line(input);
    }
});
