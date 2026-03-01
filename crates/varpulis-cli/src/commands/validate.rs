use anyhow::Result;
use varpulis_parser::parse;

pub fn parse_and_show(source: &str) -> Result<()> {
    println!("Parsing VPL...\n");

    match parse(source) {
        Ok(program) => {
            println!("Parse successful!\n");
            println!("AST:");
            println!("{program:#?}");
        }
        Err(e) => {
            println!("Parse error: {e}");
        }
    }

    Ok(())
}

pub fn check_syntax(source: &str) -> Result<()> {
    match parse(source) {
        Ok(program) => {
            // Run semantic validation
            let validation = varpulis_core::validate::validate(source, &program);
            let errors: Vec<_> = validation
                .diagnostics
                .iter()
                .filter(|d| d.severity == varpulis_core::validate::Severity::Error)
                .collect();
            let warnings: Vec<_> = validation
                .diagnostics
                .iter()
                .filter(|d| d.severity == varpulis_core::validate::Severity::Warning)
                .collect();

            if errors.is_empty() {
                println!("Syntax OK");
                println!("   Statements: {}", program.statements.len());
                if !warnings.is_empty() {
                    println!("   Warnings:   {}", warnings.len());
                    for w in &warnings {
                        let (line, col) =
                            varpulis_core::validate::diagnostic_position(source, w.span.start);
                        let code_str = w.code.map(|c| format!("[{c}] ")).unwrap_or_default();
                        println!("   {}:{}: warning: {}{}", line, col, code_str, w.message);
                        if let Some(ref hint) = w.hint {
                            println!("      hint: {hint}");
                        }
                    }
                }
            } else {
                println!("Validation failed:");
                println!("   Errors:   {}", errors.len());
                for e in &errors {
                    let (line, col) =
                        varpulis_core::validate::diagnostic_position(source, e.span.start);
                    let code_str = e.code.map(|c| format!("[{c}] ")).unwrap_or_default();
                    println!("   {}:{}: error: {}{}", line, col, code_str, e.message);
                    if let Some(ref hint) = e.hint {
                        println!("      hint: {hint}");
                    }
                }
                if !warnings.is_empty() {
                    println!("   Warnings: {}", warnings.len());
                    for w in &warnings {
                        let (line, col) =
                            varpulis_core::validate::diagnostic_position(source, w.span.start);
                        let code_str = w.code.map(|c| format!("[{c}] ")).unwrap_or_default();
                        println!("   {}:{}: warning: {}{}", line, col, code_str, w.message);
                        if let Some(ref hint) = w.hint {
                            println!("      hint: {hint}");
                        }
                    }
                }
                std::process::exit(1);
            }
        }
        Err(e) => {
            println!("Syntax error: {e}");

            // Show context around the error if we have a Located error
            if let varpulis_parser::ParseError::Located {
                line, column, hint, ..
            } = &e
            {
                // Show hint if available
                if let Some(h) = hint {
                    println!("   Hint: {h}");
                }

                // Show the problematic line from source
                if let Some(error_line) = source.lines().nth(line - 1) {
                    println!("   |");
                    println!("   | {error_line}");
                    println!("   | {}^", " ".repeat(column.saturating_sub(1)));
                }
            }

            std::process::exit(1);
        }
    }
    Ok(())
}
