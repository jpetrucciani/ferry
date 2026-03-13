{ pkgs ? import
  (fetchTarball {
    name = "jpetrucciani-2026-03-06";
    url = "https://github.com/jpetrucciani/nix/archive/2747da7206c6a736a3ec040b30e7e2b01ec02a40.tar.gz";
    sha256 = "08flih0r4kc9x3pqj48g9ybpvhkrmcq0lz184h1jfn0k65hybfz6";
  })
  { overlays = [ _rust ]; }
, _rust ? import
  (fetchTarball {
    name = "oxalica-2026-03-06";
    url = "https://github.com/oxalica/rust-overlay/archive/629bbb7f9d02787a54e28398b411da849246253b.tar.gz";
    sha256 = "1bfp6815dll8b9r5n3r55f9f9ayibm66w8nrzzyqd4bwhd2kvqlb";
  })
}:
let
  name = "ferry";

  target = "x86_64-unknown-linux-musl";
  rust = pkgs.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default.override {
    extensions = [ "rust-src" "rustc-dev" "rust-analyzer" ];
    targets = ["x86_64-pc-windows-gnu"  target ];
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
