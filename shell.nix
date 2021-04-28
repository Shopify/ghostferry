with (import <nixpkgs> {});
let
  ruby = ruby_2_7;
  env = bundlerEnv {
    name = "ghostferry-bundler-env";
    ruby = ruby;
    gemfile  = ./Gemfile;
    lockfile = ./Gemfile.lock;
    gemset   = ./gemset.nix;
  };
in stdenv.mkDerivation {
  name = "ghostferry";
  buildInputs = [
    env
    ruby
    go_1_16
    mysql57
  ];
}
