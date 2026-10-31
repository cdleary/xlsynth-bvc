// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::model::{ActionSpec, ArtifactType};
use crate::store::ArtifactStore;
use std::time::{SystemTime, UNIX_EPOCH};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "bvc-ir-archive-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        Self(root)
    }
    fn site(&self) -> PathBuf {
        self.0.join("site")
    }
    fn store(&self) -> ArtifactStore {
        let store = ArtifactStore::new_with_sled(self.0.join("store"), self.0.join("store.sled"));
        store.ensure_layout().unwrap();
        store
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn ir() -> &'static [u8] {
    b"package test\ntop fn f(x: bits[1] id=1) -> bits[1] {\n ret not.2: bits[1] = not(x, id=2)\n}\n"
}

fn root(corpus: &str, path: &str) -> wire::IrArchiveRoot {
    wire::IrArchiveRoot {
        corpus: corpus.into(),
        source_relpath: path.into(),
        source_sha256: digest(ir()),
        top_fn_name: "f".into(),
    }
}

fn fixture_builder() -> Builder {
    let mut builder = Builder::default();
    builder.add(root("full-corpus", "hash/f.ir"), ir()).unwrap();
    builder
        .add(root("whole-functions-v1", "structural.ir"), ir())
        .unwrap();
    builder
}

fn index(site: &Path) -> wire::IrArchiveIndex {
    wire::IrArchiveIndex::decode(fs::read(site.join(INDEX)).unwrap().as_slice()).unwrap()
}

fn replace_pack(site: &Path, bytes: Vec<u8>) {
    let mut index = index(site);
    let bucket = &mut index.buckets[digest(ir())[0] as usize];
    bucket.archive_bytes = bytes.len() as u64;
    bucket.archive_sha256 = digest(&bytes);
    fs::write(site.join(archive_path(&bucket.prefix)), bytes).unwrap();
    fs::write(site.join(INDEX), index.encode_to_vec()).unwrap();
}

#[test]
fn archives_are_fixed_deterministic_and_deduplicated() {
    let fixture = Fixture::new();
    let first = fixture_builder();
    first.write(&fixture.site()).unwrap();
    let index = index(&fixture.site());
    assert_eq!(index.buckets.len(), 256);
    assert_eq!(index.roots.len(), 2);
    assert_eq!(
        index.buckets.iter().map(|b| b.blobs.len()).sum::<usize>(),
        1
    );
    assert_eq!(first.verify(&fixture.site()).unwrap().len(), 257);
    let mut reordered = Builder::default();
    reordered
        .add(root("whole-functions-v1", "structural.ir"), ir())
        .unwrap();
    reordered
        .add(root("full-corpus", "hash/f.ir"), ir())
        .unwrap();
    reordered
        .add(root("full-corpus", "hash/f.ir"), ir())
        .unwrap();
    reordered.verify(&fixture.site()).unwrap();
    let (_, blobs, _) = read_bundle(&Source::Local(fixture.site())).unwrap();
    assert_eq!(blobs, BTreeMap::from([(digest(ir()), ir().to_vec())]));
    let mut roundtrip = Builder::default();
    roundtrip.extend_from_site(&fixture.site()).unwrap();
    roundtrip.verify(&fixture.site()).unwrap();
}

#[test]
fn progression_download_roots_must_match_independent_evidence() {
    let fixture = Fixture::new();
    let combined = fixture_builder();
    combined.write(&fixture.site()).unwrap();
    let mut corpus = Builder::default();
    corpus.add(root("full-corpus", "hash/f.ir"), ir()).unwrap();
    let progression = root("whole-functions-v1", "structural.ir");
    assert_eq!(
        corpus
            .verify_including(&fixture.site(), &[progression.clone()])
            .unwrap()
            .len(),
        257
    );
    assert!(corpus.verify_including(&fixture.site(), &[]).is_err());
    let mut wrong = progression;
    wrong.source_sha256 = vec![0u8; 32];
    assert!(corpus.verify_including(&fixture.site(), &[wrong]).is_err());
}

#[test]
fn import_populates_the_same_canonical_cache_roots_and_is_idempotent() {
    let fixture = Fixture::new();
    fixture_builder().write(&fixture.site()).unwrap();
    let store = fixture.store();
    let out = fixture.0.join("download");
    let summary = import(&store, Some(&fixture.site()), None, &out, &[]).unwrap();
    assert_eq!(
        (
            summary.roots,
            summary.unique_ir_blobs,
            summary.imported_actions,
            summary.already_cached_actions
        ),
        (2, 1, 1, 0)
    );
    assert_eq!(
        fs::read(out.join("inputs/full-corpus/hash/f.ir")).unwrap(),
        ir()
    );
    assert_eq!(
        fs::read(out.join("inputs/whole-functions-v1/structural.ir")).unwrap(),
        ir()
    );
    let action = ActionSpec::ImportIrPackageFile {
        source_sha256: hex::encode(digest(ir())),
        top_fn_name: Some("f".into()),
    };
    let id = crate::executor::compute_action_id(&action).unwrap();
    let listing: ImportedRoots =
        serde_json::from_slice(&fs::read(out.join("roots.json")).unwrap()).unwrap();
    assert_eq!(listing.schema_version, 1);
    assert_eq!(listing.roots.len(), 2);
    assert!(listing.roots.iter().all(|root| root.import_action_id == id));
    let provenance = store.load_provenance(&id).unwrap();
    assert_eq!(
        crate::proto::action_spec_to_proto(&provenance.action).unwrap(),
        crate::proto::action_spec_to_proto(&action).unwrap()
    );
    assert!(provenance.dependencies.is_empty());
    assert_eq!(
        provenance.output_artifact.artifact_type,
        ArtifactType::IrPackageFile
    );
    assert_eq!(
        fs::read(store.resolve_artifact_ref_path(&provenance.output_artifact)).unwrap(),
        ir()
    );
    let second = import(
        &store,
        Some(&fixture.site()),
        None,
        &fixture.0.join("download-again"),
        &[],
    )
    .unwrap();
    assert_eq!(
        (second.imported_actions, second.already_cached_actions),
        (0, 1)
    );
    assert!(import(&store, Some(&fixture.site()), None, &out, &[]).is_err());
    assert_eq!(
        fs::read(out.join("inputs/full-corpus/hash/f.ir")).unwrap(),
        ir()
    );
}

#[test]
fn rejects_unknown_fields_bad_root_paths_and_wrong_functions() {
    let fixture = Fixture::new();
    fixture_builder().write(&fixture.site()).unwrap();
    let store = fixture.store();
    let path = fixture.site().join(INDEX);
    let valid = fs::read(&path).unwrap();
    let mut unknown = valid.clone();
    unknown.extend_from_slice(&[0xa0, 0x06, 0x01]);
    fs::write(&path, unknown).unwrap();
    assert!(
        import(
            &store,
            Some(&fixture.site()),
            None,
            &fixture.0.join("bad-wire"),
            &[]
        )
        .is_err()
    );
    for bad in [
        "../escape.ir",
        "/absolute.ir",
        "a/../../escape.ir",
        "a//b.ir",
        "a\\b.ir",
        "C:/bad.ir",
    ] {
        let mut index = wire::IrArchiveIndex::decode(valid.as_slice()).unwrap();
        index.roots[0].source_relpath = bad.into();
        fs::write(&path, index.encode_to_vec()).unwrap();
        assert!(
            import(
                &store,
                Some(&fixture.site()),
                None,
                &fixture.0.join("bad-path"),
                &[]
            )
            .is_err()
        );
    }
    let mut index = wire::IrArchiveIndex::decode(valid.as_slice()).unwrap();
    index.roots[0].top_fn_name = "missing".into();
    fs::write(&path, index.encode_to_vec()).unwrap();
    assert!(
        import(
            &store,
            Some(&fixture.site()),
            None,
            &fixture.0.join("bad-top"),
            &[]
        )
        .is_err()
    );
    assert!(!fixture.0.join("escape.ir").exists());
}

#[test]
fn rejects_oversized_and_misplaced_index_records() {
    let (valid, _) = fixture_builder().packs().unwrap();
    let mut bad = valid.clone();
    bad.buckets.swap(0, 1);
    assert!(validate_index(&bad).is_err());
    let mut bad = valid.clone();
    bad.buckets[0].archive_bytes = MAX_ASSET_BYTES + 1;
    assert!(validate_index(&bad).is_err());
    let mut bad = valid.clone();
    bad.buckets[digest(ir())[0] as usize].blobs[0].bytes = MAX_ASSET_BYTES + 1;
    assert!(validate_index(&bad).is_err());
    let mut bad = valid;
    bad.roots.clear();
    assert!(validate_index(&bad).is_err());
}

#[test]
fn rejects_overlapping_root_paths_and_expanded_output_bombs() {
    let (mut index, _) = fixture_builder().packs().unwrap();
    index.roots = vec![
        root("full-corpus", "file.ir"),
        root("full-corpus", "file.ir/child.ir"),
    ];
    assert!(validate_index(&index).is_err());
    let (mut index, _) = fixture_builder().packs().unwrap();
    index.buckets[digest(ir())[0] as usize].blobs[0].bytes = MAX_ASSET_BYTES;
    index.roots = (0..42)
        .map(|n| root("full-corpus", &format!("{n:02}.ir")))
        .collect();
    assert!(validate_index(&index).is_err());
}

#[test]
fn rejects_zstd_expansion_beyond_declared_tar_size() {
    let fixture = Fixture::new();
    fixture_builder().write(&fixture.site()).unwrap();
    let inflated = vec![0u8; 1024 * 1024];
    replace_pack(
        &fixture.site(),
        zstd::stream::encode_all(inflated.as_slice(), 3).unwrap(),
    );
    assert!(read_bundle(&Source::Local(fixture.site())).is_err());
}

#[test]
fn writer_rejects_a_pack_over_the_hosting_limit() {
    // Exercise packing independently from root admission with deterministic,
    // incompressible fixture bytes, not a timing-based assertion.
    let mut state = 0x0123456789abcdefu64;
    let mut bytes = vec![0u8; MAX_ASSET_BYTES as usize + 1024];
    for byte in &mut bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    let builder = Builder {
        blobs: BTreeMap::from([(vec![0u8; 32], bytes)]),
        roots: BTreeMap::new(),
    };
    let error = builder
        .packs()
        .err()
        .expect("oversized compressed pack must fail");
    assert_eq!(
        error.to_string(),
        "IR archive 00.tar.zst exceeds the 25 MiB hosting limit"
    );
}

struct HttpFixture {
    url: String,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl HttpFixture {
    fn new(site: &Path, redirect: bool) -> Self {
        use std::io::{BufRead, Write};
        use std::net::TcpListener;
        use std::sync::{Arc, atomic::AtomicBool};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let mut files = BTreeMap::new();
        for path in fixture_builder().verify(site).unwrap() {
            files.insert(
                format!("/published/{path}"),
                fs::read(site.join(path)).unwrap(),
            );
        }
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let thread_stop = stop.clone();
        let thread_requests = requests.clone();
        let thread = std::thread::spawn(move || {
            for stream in listener.incoming() {
                if thread_stop.load(Ordering::SeqCst) {
                    break;
                }
                let mut stream = stream.unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let path = line.split_whitespace().nth(1).unwrap().to_string();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                }
                thread_requests.fetch_add(1, Ordering::SeqCst);
                if redirect {
                    stream.write_all(b"HTTP/1.1 302 Found\r\nLocation: /elsewhere\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                } else if let Some(bytes) = files.get(&path) {
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        bytes.len()
                    )
                    .unwrap();
                    stream.write_all(bytes).unwrap();
                } else {
                    stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                }
            }
        });
        Self {
            url: format!("http://{address}/published"),
            stop,
            requests,
            thread: Some(thread),
        }
    }
}

impl Drop for HttpFixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let url = reqwest::Url::parse(&self.url).unwrap();
        let _ = std::net::TcpStream::connect((url.host_str().unwrap(), url.port().unwrap()));
        self.thread.take().unwrap().join().unwrap();
    }
}

#[test]
fn imports_from_http_and_rejects_redirects() {
    let fixture = Fixture::new();
    fixture_builder().write(&fixture.site()).unwrap();
    let store = fixture.store();
    let server = HttpFixture::new(&fixture.site(), false);
    let output = fixture.0.join("remote");
    let summary = import(&store, None, Some(&server.url), &output, &[]).unwrap();
    assert_eq!((summary.imported_actions, summary.unique_ir_blobs), (1, 1));
    assert_eq!(server.requests.load(Ordering::SeqCst), 257);
    assert_eq!(
        fs::read(output.join("inputs/full-corpus/hash/f.ir")).unwrap(),
        ir()
    );
    let redirect = HttpFixture::new(&fixture.site(), true);
    let redirected = fixture.0.join("redirected");
    assert!(import(&store, None, Some(&redirect.url), &redirected, &[]).is_err());
    assert_eq!(redirect.requests.load(Ordering::SeqCst), 1);
    assert!(!redirected.exists());
}

#[test]
fn archive_cli_requires_one_source_and_an_output() {
    use clap::Parser;
    let parse = |args: &[&str]| {
        crate::cli::Cli::try_parse_from(std::iter::once("bvc").chain(args.iter().copied()))
    };
    assert!(parse(&["import-ir-archives", "--out-dir", "out"]).is_err());
    assert!(parse(&["import-ir-archives", "--site-dir", "site"]).is_err());
    assert!(
        parse(&[
            "import-ir-archives",
            "--site-dir",
            "site",
            "--url",
            "https://example.invalid/",
            "--out-dir",
            "out"
        ])
        .is_err()
    );
    let parsed = parse(&[
        "import-ir-archives",
        "--site-dir",
        "site",
        "--out-dir",
        "out",
    ])
    .unwrap();
    let crate::cli::TopCommand::ImportIrArchives {
        site_dir,
        url,
        out_dir,
    } = parsed.command
    else {
        panic!("wrong command");
    };
    assert_eq!(
        (site_dir, url, out_dir),
        (Some(PathBuf::from("site")), None, PathBuf::from("out"))
    );
}

#[test]
fn rejects_corrupt_archives_even_before_importing_any_root() {
    let fixture = Fixture::new();
    let builder = fixture_builder();
    builder.write(&fixture.site()).unwrap();
    let path = fixture
        .site()
        .join(archive_path(&format!("{:02x}", digest(ir())[0])));
    fs::write(&path, b"corrupt zstd").unwrap();
    let store = fixture.store();
    let output = fixture.0.join("bad");
    assert!(import(&store, Some(&fixture.site()), None, &output, &[]).is_err());
    assert!(!output.exists());
    let action = ActionSpec::ImportIrPackageFile {
        source_sha256: hex::encode(digest(ir())),
        top_fn_name: Some("f".into()),
    };
    assert!(!store.action_exists(&crate::executor::compute_action_id(&action).unwrap()));
}

#[test]
fn rejects_rehashed_malicious_tar_members_and_wrong_blob_bytes() {
    let fixture = Fixture::new();
    let store = fixture.store();
    for kind in [
        tar::EntryType::Symlink,
        tar::EntryType::Link,
        tar::EntryType::Directory,
        tar::EntryType::Regular,
    ] {
        fixture_builder().write(&fixture.site()).unwrap();
        let mut tar = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(if kind == tar::EntryType::Regular {
            ir().len() as u64
        } else {
            0
        });
        header.set_entry_type(kind);
        if matches!(kind, tar::EntryType::Symlink | tar::EntryType::Link) {
            header.set_link_name("outside.ir").unwrap();
        }
        header.set_cksum();
        let data = if kind == tar::EntryType::Regular {
            ir()
        } else {
            b""
        };
        tar.append_data(&mut header, "unexpected.ir", data).unwrap();
        let packed = zstd::stream::encode_all(tar.into_inner().unwrap().as_slice(), 3).unwrap();
        replace_pack(&fixture.site(), packed);
        assert!(
            import(
                &store,
                Some(&fixture.site()),
                None,
                &fixture.0.join("malicious"),
                &[]
            )
            .is_err()
        );
    }
    fixture_builder().write(&fixture.site()).unwrap();
    let bad = tar_bytes([(digest(ir()), vec![b'x'; ir().len()])]).unwrap();
    replace_pack(
        &fixture.site(),
        zstd::stream::encode_all(bad.as_slice(), 3).unwrap(),
    );
    assert!(
        import(
            &store,
            Some(&fixture.site()),
            None,
            &fixture.0.join("wrong-ir"),
            &[]
        )
        .is_err()
    );
}

#[cfg(unix)]
#[test]
fn rejects_source_symlinks_and_protected_output_overlap() {
    let fixture = Fixture::new();
    fixture_builder().write(&fixture.site()).unwrap();
    let store = fixture.store();
    let original = fs::read(fixture.site().join(INDEX)).unwrap();
    fs::write(fixture.0.join("external.pb"), original).unwrap();
    fs::remove_file(fixture.site().join(INDEX)).unwrap();
    std::os::unix::fs::symlink(fixture.0.join("external.pb"), fixture.site().join(INDEX)).unwrap();
    assert!(
        import(
            &store,
            Some(&fixture.site()),
            None,
            &fixture.0.join("bad-link"),
            &[]
        )
        .is_err()
    );
    assert!(
        import(
            &store,
            Some(&fixture.site()),
            None,
            &fixture.site().join("nested"),
            &[]
        )
        .is_err()
    );
    assert!(
        import(
            &store,
            Some(&fixture.site()),
            None,
            &store.root.join("download"),
            &[("store", store.root.as_path())]
        )
        .is_err()
    );
}
