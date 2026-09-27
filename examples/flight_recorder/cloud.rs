//! Object storage for archived tables: a local directory, an in-memory
//! bucket (the torture test), or S3.
//!
//! S3 goes through the `curl` command line (`--aws-sigv4`), so the example
//! needs no crates: horton itself has no dependencies, and neither does
//! its demo. Credentials come from the usual environment variables
//! (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional
//! `AWS_SESSION_TOKEN`, `AWS_REGION`). `AWS_ENDPOINT_URL` points at any
//! S3-compatible service (`MinIO`, Cloudflare R2, `LocalStack`, …) with
//! path-style URLs; without it, AWS virtual-hosted URLs are used.
//!
//! Every store writes an object atomically: readers see the whole object or
//! none of it. That is what makes the archive protocol crash-safe: the
//! table is committed away locally only after its object exists.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};

/// Where archived tables go.
pub trait ObjectStore {
    /// A human-readable location, for example `s3://bucket/prefix`.
    fn location(&self) -> String;

    /// Stores `key`, streaming the body from `fill`. The object appears
    /// only if `fill` and the upload both succeed.
    fn put(
        &mut self,
        key: &str,
        fill: &mut dyn FnMut(&mut dyn Write) -> io::Result<()>,
    ) -> io::Result<()>;

    /// The whole object stored at `key`.
    fn get(&self, key: &str) -> io::Result<Vec<u8>>;

    /// Every key under `prefix`, sorted.
    fn list(&self, prefix: &str) -> io::Result<Vec<String>>;
}

/// A bucket in a local directory: one file per object, written to a
/// temporary name and renamed into place.
pub struct DirBucket {
    root: PathBuf,
}

impl DirBucket {
    pub fn new(root: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }
}

impl ObjectStore for DirBucket {
    fn location(&self) -> String {
        format!("{}/", self.root.display())
    }

    fn put(
        &mut self,
        key: &str,
        fill: &mut dyn FnMut(&mut dyn Write) -> io::Result<()>,
    ) -> io::Result<()> {
        let path = self.root.join(key);
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension("partial");
        let mut out = io::BufWriter::new(fs::File::create(&tmp)?);
        fill(&mut out)?;
        out.into_inner()
            .map_err(io::IntoInnerError::into_error)?
            .sync_all()?;
        fs::rename(&tmp, &path)
    }

    fn get(&self, key: &str) -> io::Result<Vec<u8>> {
        fs::read(self.root.join(key))
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let dir = self.root.join(prefix);
        let mut keys = Vec::new();
        if dir.is_dir() {
            for entry in fs::read_dir(&dir)? {
                let name = entry?.file_name().to_string_lossy().into_owned();
                if !name.ends_with(".partial") {
                    keys.push(format!("{prefix}{name}"));
                }
            }
        }
        keys.sort();
        Ok(keys)
    }
}

/// A bucket in RAM, for the power-cut test.
#[derive(Default)]
pub struct MemBucket {
    objects: BTreeMap<String, Vec<u8>>,
}

impl ObjectStore for MemBucket {
    fn location(&self) -> String {
        "memory".into()
    }

    fn put(
        &mut self,
        key: &str,
        fill: &mut dyn FnMut(&mut dyn Write) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut body = Vec::new();
        fill(&mut body)?;
        self.objects.insert(key.to_owned(), body);
        Ok(())
    }

    fn get(&self, key: &str) -> io::Result<Vec<u8>> {
        self.objects
            .get(key)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, key.to_owned()))
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        Ok(self
            .objects
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }
}

/// An S3 bucket, spoken to through `curl --aws-sigv4`.
pub struct S3Bucket {
    bucket: String,
    prefix: String,
    region: String,
    endpoint: Option<String>,
    user: String,
    token: Option<String>,
}

impl S3Bucket {
    /// Parses `s3://bucket/prefix` and reads credentials from the
    /// environment.
    pub fn from_url(url: &str) -> Result<Self, String> {
        let rest = url
            .strip_prefix("s3://")
            .ok_or_else(|| format!("expected s3://bucket/prefix, got {url}"))?;
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(format!("no bucket in {url}"));
        }
        let env = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        let id = env("AWS_ACCESS_KEY_ID").ok_or("AWS_ACCESS_KEY_ID is not set")?;
        let secret = env("AWS_SECRET_ACCESS_KEY").ok_or("AWS_SECRET_ACCESS_KEY is not set")?;
        let prefix = prefix.trim_end_matches('/');
        Ok(Self {
            bucket: bucket.to_owned(),
            prefix: if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}/")
            },
            region: env("AWS_REGION")
                .or_else(|| env("AWS_DEFAULT_REGION"))
                .unwrap_or_else(|| "us-east-1".into()),
            endpoint: env("AWS_ENDPOINT_URL").map(|e| e.trim_end_matches('/').to_owned()),
            user: format!("{id}:{secret}"),
            token: env("AWS_SESSION_TOKEN"),
        })
    }

    /// The URL of the bucket root; object keys and queries follow it.
    fn bucket_url(&self) -> String {
        self.endpoint.as_ref().map_or_else(
            || format!("https://{}.s3.{}.amazonaws.com", self.bucket, self.region),
            |endpoint| format!("{endpoint}/{}", self.bucket),
        )
    }

    /// A signed `curl` invocation for `url`.
    fn curl(&self, url: &str) -> Command {
        let mut cmd = Command::new("curl");
        cmd.args(["--silent", "--show-error", "--fail-with-body"])
            .args(["--aws-sigv4", &format!("aws:amz:{}:s3", self.region)])
            .args(["--user", &self.user])
            .args(["-H", "x-amz-content-sha256: UNSIGNED-PAYLOAD"]);
        if let Some(token) = &self.token {
            cmd.args(["-H", &format!("x-amz-security-token: {token}")]);
        }
        cmd.arg(url);
        cmd
    }

    fn run(mut cmd: Command) -> io::Result<Vec<u8>> {
        let out = cmd.stdin(Stdio::null()).output()?;
        if out.status.success() {
            Ok(out.stdout)
        } else {
            Err(io::Error::other(format!(
                "curl failed ({}): {}{}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim(),
                String::from_utf8_lossy(&out.stdout).trim()
            )))
        }
    }
}

impl ObjectStore for S3Bucket {
    fn location(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }

    fn put(
        &mut self,
        key: &str,
        fill: &mut dyn FnMut(&mut dyn Write) -> io::Result<()>,
    ) -> io::Result<()> {
        // The body streams into curl's stdin block by block; curl sends it
        // as one PUT, which S3 applies atomically.
        let url = format!("{}/{}{key}", self.bucket_url(), self.prefix);
        let mut cmd = self.curl(&url);
        cmd.args(["-X", "PUT", "--data-binary", "@-"])
            .args(["-H", "Content-Type: application/octet-stream"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn()?;
        let mut stdin = child.stdin.take().expect("stdin is piped");
        let filled = fill(&mut stdin);
        drop(stdin);
        let out = child.wait_with_output()?;
        filled?;
        if out.status.success() {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "S3 PUT {key} failed ({}): {}{}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim(),
                String::from_utf8_lossy(&out.stdout).trim()
            )))
        }
    }

    fn get(&self, key: &str) -> io::Result<Vec<u8>> {
        Self::run(self.curl(&format!("{}/{}{key}", self.bucket_url(), self.prefix)))
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        // ListObjectsV2, following continuation tokens. The keys are plain
        // ASCII, so a string search of the XML is enough.
        let full = format!("{}{prefix}", self.prefix);
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut url = format!("{}?list-type=2&prefix={}", self.bucket_url(), encode(&full));
            if let Some(t) = &token {
                url.push_str("&continuation-token=");
                url.push_str(&encode(t));
            }
            let xml = String::from_utf8_lossy(&Self::run(self.curl(&url))?).into_owned();
            for key in tag_values(&xml, "Key") {
                if let Some(k) = key.strip_prefix(&self.prefix) {
                    keys.push(k.to_owned());
                }
            }
            token = tag_values(&xml, "NextContinuationToken").into_iter().next();
            if token.is_none() {
                break;
            }
        }
        keys.sort();
        Ok(keys)
    }
}

/// Percent-encodes a query value.
fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// The text of every `<tag>…</tag>` in `xml`.
fn tag_values(xml: &str, tag: &str) -> Vec<String> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(&close) else { break };
        out.push(after[..j].to_owned());
        rest = &after[j + close.len()..];
    }
    out
}
