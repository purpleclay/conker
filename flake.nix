{
  description = "Concurrency with the string attached";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    go-overlay = {
      url = "github:purpleclay/go-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
    };

    git-hooks = {
      url = "github:cachix/git-hooks.nix";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };
  };

  outputs = {
    nixpkgs,
    flake-utils,
    go-overlay,
    git-hooks,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [go-overlay.overlays.default];
        };
        go = pkgs.go-bin.fromGoMod ./go.mod;

        pre-commit-check = git-hooks.lib.${system}.run {
          src = ./.;
          package = pkgs.prek;
          hooks = {
            alejandra = {
              enable = true;
              settings = {
                check = true;
              };
            };

            typos = {
              enable = true;
              entry = "${pkgs.typos}/bin/typos --force-exclude";
            };
          };
        };
      in
        with pkgs; {
          devShells.default = mkShell {
            inherit (pre-commit-check) shellHook;
            buildInputs =
              [
                alejandra
                (go.withTools ["golangci-lint" "gopls" "gofumpt" "staticcheck"])
                nil
                typos
              ]
              ++ pre-commit-check.enabledPackages;
          };
        }
    );
}
