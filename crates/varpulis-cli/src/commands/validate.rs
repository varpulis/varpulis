use anyhow::Result;
use varpulis_core::validate::RichDiagnostic;
use varpulis_parser::{parse, RichParseError};

pub fn parse_and_show(source: &str, filename: &str) -> Result<()> {
    println!("Parsing VPL...\n");

    match parse(source) {
        Ok(program) => {
            println!("Parse successful!\n");
            println!("AST:");
            println!("{program:#?}");
        }
        Err(e) => {
            let rich = RichParseError::new(e, source, filename);
            let report = miette::Report::new(rich);
            println!("{report:?}");
        }
    }

    Ok(())
}

pub fn check_syntax(source: &str, filename: &str) -> Result<()> {
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
                        let rich = RichDiagnostic::from_diagnostic(w, source, filename);
                        let report = miette::Report::new(rich);
                        println!("{report:?}");
                    }
                }
            } else {
                println!("Validation failed:");
                println!("   Errors:   {}", errors.len());
                for e in &errors {
                    let rich = RichDiagnostic::from_diagnostic(e, source, filename);
                    let report = miette::Report::new(rich);
                    println!("{report:?}");
                }
                if !warnings.is_empty() {
                    println!("   Warnings: {}", warnings.len());
                    for w in &warnings {
                        let rich = RichDiagnostic::from_diagnostic(w, source, filename);
                        let report = miette::Report::new(rich);
                        println!("{report:?}");
                    }
                }
                std::process::exit(1);
            }
        }
        Err(e) => {
            let rich = RichParseError::new(e, source, filename);
            let report = miette::Report::new(rich);
            println!("{report:?}");
            std::process::exit(1);
        }
    }
    Ok(())
}
