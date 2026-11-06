{
  inputs.nixpkgs.url = "flake:nixpkgs";

  outputs =
    { nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      redpanda = pkgs.callPackage ./nix/redpanda.nix { };
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        # The integration tests start a local broker.
        nativeBuildInputs = [
          redpanda.redpanda
          redpanda.rpk
        ];
      };
    };
}
