# Redpanda and rpk for the integration tests. Stripped copy of
# /share/infra/nix/redpanda.nix: keep the version equal to production.
{
  stdenv,
  autoPatchelfHook,
  dpkg,
  openssl,
  e2fsprogs,
  krb5,
}:
let
  version = "25.3.17-1";
  fromDeb =
    pname: hash: sha256: buildInputs:
    stdenv.mkDerivation {
      inherit pname version buildInputs;
      src = builtins.fetchurl {
        url = "https://linux.pkg.redpanda.com/apt/pool/redpanda-apt/${pname}_${version}_amd64_${hash}.deb";
        inherit sha256;
      };
      nativeBuildInputs = [
        dpkg
        autoPatchelfHook
        stdenv.cc.cc.lib
      ];
      unpackPhase = "dpkg-deb --fsys-tarfile $src | tar -x --no-same-owner";
      installPhase = ''
        mkdir -p $out/bin
        chmod +x opt/redpanda/libexec/*
        mv opt/redpanda/libexec/* $out/bin
      '';
    };
in
{
  redpanda = fromDeb "redpanda" "c288969ffd69f118ace7e2b33bd54efe" "38f109b8f85152d80086dc5cdad6af432f4007297004b863a3007aa8a91455a3" [
    openssl
    e2fsprogs
    krb5
  ];
  rpk = fromDeb "redpanda-rpk" "c65e9c832c8d5622382f09a5fd8b1763" "f93e2be43c479a6261ff335147b3e04773f007c659b1500b1084f2f4fdd0aab9" [
    openssl
  ];
}
