mod logging;
mod subcommands;

use std::{fs::OpenOptions, process};

use clap::{crate_description, crate_version, Arg, Command};

const LOGGING: &str = "logging";

fn cli() -> Command<'static> {
    Command::new("casper-db-utils")
        .version(crate_version!())
        .about(crate_description!())
        .arg_required_else_help(true)
        .subcommand(subcommands::check::command(0))
        .subcommand(subcommands::trie_compact::command(1))
        .subcommand(subcommands::unsparsify::command(2))
        .arg(
            Arg::new(LOGGING)
                .short('l')
                .long(LOGGING)
                .takes_value(true)
                .value_name("LOGFILE_PATH")
                .help("Path to file where program will dump log messages."),
        )
}

fn main() {
    let arg_matches = cli().get_matches();

    // Initialize logger.
    arg_matches.value_of(LOGGING).map_or_else(
        || logging::init_term_logger().expect("Couldn't initialize terminal logger"),
        |path| {
            let logfile = OpenOptions::new()
                .append(true)
                .create(true)
                .open(path)
                .expect("Couldn't open logfile");
            let line_writer = std::io::LineWriter::new(logfile);
            logging::init_write_logger(line_writer).expect("Couldn't initialize logger to file");
        },
    );

    let (subcommand_name, matches) = arg_matches.subcommand().unwrap_or_else(|| {
        let _ = cli().print_long_help();
        println!();
        process::exit(1);
    });

    let succeeded = match subcommand_name {
        subcommands::check::COMMAND_NAME => subcommands::check::run(matches),
        subcommands::trie_compact::COMMAND_NAME => subcommands::trie_compact::run(matches),
        subcommands::unsparsify::COMMAND_NAME => subcommands::unsparsify::run(matches),
        _ => {
            let _ = cli().print_long_help();
            println!();
            process::exit(1);
        }
    };

    if !succeeded {
        process::exit(1);
    }
}
