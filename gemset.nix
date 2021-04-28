{
  ansi = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "14ims9zfal4gs2wpx2m5rd8zsrl2k794d359shkrsgg3fhr2a22l";
      type = "gem";
    };
    version = "1.5.0";
  };
  builder = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "045wzckxpwcqzrjr353cxnyaxgf0qg22jh00dcx7z38cys5g1jlr";
      type = "gem";
    };
    version = "3.2.4";
  };
  byebug = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "0nx3yjf4xzdgb8jkmk2344081gqr22pgjqnmjg2q64mj5d6r9194";
      type = "gem";
    };
    version = "11.1.3";
  };
  coderay = {
    groups = ["default" "development"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "0jvxqxzply1lwp7ysn94zjhh57vc14mcshw1ygw14ib8lhc00lyw";
      type = "gem";
    };
    version = "1.1.3";
  };
  method_source = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "1pnyh44qycnf9mzi1j6fywd5fkskv3x7nmsqrrws0rjn5dd4ayfp";
      type = "gem";
    };
    version = "1.0.0";
  };
  minitest = {
    groups = ["test"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "19z7wkhg59y8abginfrm2wzplz7py3va8fyngiigngqvsws6cwgl";
      type = "gem";
    };
    version = "5.14.4";
  };
  minitest-fail-fast = {
    dependencies = ["minitest"];
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "0kgxx7v1wyk1zaim41zim3z4s9nx1ybani2097v39h1fqn2mzdr5";
      type = "gem";
    };
    version = "0.1.0";
  };
  minitest-hooks = {
    dependencies = ["minitest"];
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "05z8r6sw3fz4s44fs1150ndlcmcy82vlxmhps5nncg8vk59k3gmf";
      type = "gem";
    };
    version = "1.5.0";
  };
  minitest-reporters = {
    dependencies = ["ansi" "builder" "minitest" "ruby-progressbar"];
    groups = ["test"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "0pj98s2rza8c38crh35lcrv59w7qr13g09swfh5aqabl3552xxr6";
      type = "gem";
    };
    version = "1.4.3";
  };
  mysql2 = {
    groups = ["test"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "0d14pcy5m4hjig0zdxnl9in5f4izszc7v9zcczf2gyi5kiyxk8jw";
      type = "gem";
    };
    version = "0.5.3";
  };
  pry = {
    dependencies = ["coderay" "method_source"];
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "0iyw4q4an2wmk8v5rn2ghfy2jaz9vmw2nk8415nnpx2s866934qk";
      type = "gem";
    };
    version = "0.13.1";
  };
  pry-byebug = {
    dependencies = ["byebug" "pry"];
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "096y5vmzpyy4x9h4ky4cs4y7d19vdq9vbwwrqafbh5gagzwhifiv";
      type = "gem";
    };
    version = "3.9.0";
  };
  ruby-progressbar = {
    groups = ["default" "test"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "02nmaw7yx9kl7rbaan5pl8x5nn0y4j5954mzrkzi9i3dhsrps4nc";
      type = "gem";
    };
    version = "1.11.0";
  };
  tqdm = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "1xmc2y990s0k3yydijyqifkgirzhlicb0p7b12habcksfs0h4rna";
      type = "gem";
    };
    version = "0.3.0";
  };
}
