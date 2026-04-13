{ pkgs ? import
    (fetchTarball {
      name = "jpetrucciani-2026-04-01";
      url = "https://github.com/jpetrucciani/nix/archive/9a3a8de1ca81970ebf64c773c3ca9aa0301d7c90.tar.gz";
      sha256 = "0gxfdzrwz9bb42byinwsjhmi72yifimhypjbv9v9g6haxza46l5y";
    })
    { overlays = [ rustOverlay ]; }
, rustOverlay ? import
    (fetchTarball {
      name = "oxalica-2026-04-01";
      url = "https://github.com/oxalica/rust-overlay/archive/e8046c1d9ccadd497c2344d8fa49dab62f22f7be.tar.gz";
      sha256 = "0371f7g0sans86jbx4pkvcdmzn46fyk63mln7g8vzmjchgmfvync";
    })
}:
let
  name = "ferry";

  target = "x86_64-unknown-linux-musl";
  rust = pkgs.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default.override {
    extensions = [ "rust-src" "rustc-dev" "rust-analyzer" ];
    targets = [ "x86_64-pc-windows-gnu" target ];
  });

  rustPlatform = pkgs.makeRustPlatform {
    cargo = rust;
    rustc = rust;
  };

  mingw = pkgs.pkgsCross.mingwW64;

  tools = with pkgs; {
    cli = [
      jfmt
    ];
    node = [ bun ];
    rust = [
      cargo-zigbuild
      rust
      pkg-config
      mingw.stdenv.cc
      mingw.windows.pthreads
    ];
    scripts = pkgs.lib.attrsets.attrValues scripts;
  };

  scripts = with pkgs; {
    build_static = writers.writeBashBin "build_static" ''
      cargo zigbuild --release --target "x86_64-unknown-linux-musl"
    '';
  };
  paths = pkgs.lib.flatten [ (builtins.attrValues tools) ];
  env = pkgs.buildEnv {
    inherit name paths; buildInputs = paths;
  };
  bin = rustPlatform.buildRustPackage (finalAttrs: {
    pname = name;
    version = "0.0.0";
    src = pkgs.hax.filterSrc { path = ./.; };
    cargoLock.lockFile = ./Cargo.lock;
    auditable = false;
    nativeBuildInputs = with pkgs; [
      cargo-zigbuild
    ];
    buildPhase = ''
      export HOME=$(mktemp -d)
      cargo zigbuild --release --target ${target}
    '';
    installPhase = ''
      mkdir -p $out/bin
      cp target/${target}/release/${name} $out/bin/
    '';
  });
in
(env.overrideAttrs (_: {
  inherit name;
  NIXUP = "0.0.10";
})) // { inherit bin scripts; }
