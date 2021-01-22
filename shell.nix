with (import <nixpkgs> {});
let
  env = bundlerEnv {
    name = "ghostferry-bundler-env";
    ruby = ruby_2_7;
    gemfile  = ./Gemfile;
    lockfile = ./Gemfile.lock;
    gemset   = ./gemset.nix;
  };
in stdenv.mkDerivation {
  name = "ghostferry";
  buildInputs = [
    env
    ruby
    go_1_14
    mysql57
  ];
}
