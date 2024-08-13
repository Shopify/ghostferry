with (import <nixpkgs> {});
let
  ruby = ruby_3_3_3;
  env = bundlerEnv {
    name = "ghostferry-bundler-env";
    ruby = ruby;
    gemfile  = ./Gemfile;
    lockfile = ./Gemfile.lock;
    gemset   = ./gemset.nix;
    # https://github.com/NixOS/nixpkgs/issues/83442#issuecomment-768669544
    copyGemFiles = true;
  };
in stdenv.mkDerivation {
  name = "ghostferry";
  buildInputs = [
    env
    ruby
    go_1_22_1
    mysql57
  ];
}
