use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use prediction_encoder::{CodecError, LogicalIdentity, StoredIdentity, decode_stream};

const INVOCATION_EXIT: u8 = 2;
const CODEC_EXIT: u8 = 3;
const IO_EXIT: u8 = 4;
const MAX_DIAGNOSTIC_BYTES: usize = 4095;

struct Arguments {
    input: PathBuf,
    output: PathBuf,
    logical: LogicalIdentity,
    stored: StoredIdentity,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err((category, diagnostic)) => {
            emit_diagnostic(&diagnostic);
            ExitCode::from(category)
        }
    }
}

fn run() -> Result<(), (u8, String)> {
    let arguments =
        parse_arguments(std::env::args_os().skip(1)).map_err(|detail| (INVOCATION_EXIT, detail))?;

    let metadata = std::fs::metadata(&arguments.input)
        .map_err(|error| (IO_EXIT, format!("cannot inspect input: {error}")))?;
    if !metadata.is_file() {
        return Err((IO_EXIT, "input is not a regular file".to_owned()));
    }
    let input = File::open(&arguments.input)
        .map_err(|error| (IO_EXIT, format!("cannot open input: {error}")))?;
    let mut output_options = OpenOptions::new();
    output_options.write(true).create_new(true);
    #[cfg(unix)]
    output_options.mode(0o600);
    let mut output = output_options
        .open(&arguments.output)
        .map_err(|error| (IO_EXIT, format!("cannot create output: {error}")))?;

    let result = decode_stream(
        input,
        &mut output,
        &arguments.logical,
        Some(&arguments.stored),
        Some(arguments.logical.byte_length),
    )
    .map_err(classify_codec_error)
    .and_then(|_| {
        output
            .flush()
            .map_err(|error| (IO_EXIT, format!("cannot flush output: {error}")))
    });
    drop(output);

    if let Err(error) = result {
        let _ = std::fs::remove_file(&arguments.output);
        return Err(error);
    }
    Ok(())
}

fn parse_arguments(arguments: impl Iterator<Item = OsString>) -> Result<Arguments, String> {
    const NAMES: [&str; 8] = [
        "--protocol-version",
        "--input",
        "--output",
        "--logical-sha256",
        "--logical-byte-length",
        "--logical-line-count",
        "--stored-sha256",
        "--stored-byte-length",
    ];
    let mut values = HashMap::new();
    let mut arguments = arguments;
    while let Some(name) = arguments.next() {
        let name = name
            .into_string()
            .map_err(|_| "argument name is not valid UTF-8".to_owned())?;
        if !NAMES.contains(&name.as_str()) {
            return Err(format!("unknown argument: {name}"));
        }
        if values.contains_key(name.as_str()) {
            return Err(format!("duplicate argument: {name}"));
        }
        let value = arguments
            .next()
            .ok_or_else(|| format!("missing value for {name}"))?;
        values.insert(name, value);
    }
    for name in NAMES {
        if !values.contains_key(name) {
            return Err(format!("missing argument: {name}"));
        }
    }

    let protocol = text(&values, "--protocol-version")?;
    if protocol != "1" {
        return Err("unsupported protocol version".to_owned());
    }
    let input = path(&values, "--input")?;
    let output = path(&values, "--output")?;
    Ok(Arguments {
        input,
        output,
        logical: LogicalIdentity {
            sha256: sha256(&values, "--logical-sha256")?,
            byte_length: number(&values, "--logical-byte-length")?,
            line_count: number(&values, "--logical-line-count")?,
        },
        stored: StoredIdentity {
            sha256: sha256(&values, "--stored-sha256")?,
            byte_length: number(&values, "--stored-byte-length")?,
        },
    })
}

fn text<'a>(values: &'a HashMap<String, OsString>, name: &str) -> Result<&'a str, String> {
    values[name]
        .to_str()
        .ok_or_else(|| format!("value for {name} is not valid UTF-8"))
}

fn path(values: &HashMap<String, OsString>, name: &str) -> Result<PathBuf, String> {
    let value = &values[name];
    if value.is_empty() {
        return Err(format!("empty value for {name}"));
    }
    Ok(PathBuf::from(value))
}

fn sha256(values: &HashMap<String, OsString>, name: &str) -> Result<String, String> {
    let value = text(values, name)?;
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(format!("malformed value for {name}"));
    }
    Ok(value.to_owned())
}

fn number(values: &HashMap<String, OsString>, name: &str) -> Result<u64, String> {
    let value = text(values, name)?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("malformed value for {name}"));
    }
    value
        .parse()
        .map_err(|_| format!("value for {name} is out of range"))
}

fn classify_codec_error(error: CodecError) -> (u8, String) {
    let category = if matches!(error, CodecError::Io(_)) {
        IO_EXIT
    } else {
        CODEC_EXIT
    };
    (category, error.to_string())
}

fn emit_diagnostic(diagnostic: &str) {
    let mut end = diagnostic.len().min(MAX_DIAGNOSTIC_BYTES);
    while !diagnostic.is_char_boundary(end) {
        end -= 1;
    }
    eprintln!("{}", &diagnostic[..end]);
}
