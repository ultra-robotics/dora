{ pkgs ? import <nixpkgs> { config.allowUnfree = true; } }:

pkgs.mkShell {
  name = "dora-dev";

  buildInputs = with pkgs; [
    gcc
    gcc.cc.lib
    rustc
    cargo
    python312
    python312Packages.pip
    python312Packages.setuptools
    python312Packages.wheel
    uv
    # Maturin for building Python bindings
    maturin
    # Additional build dependencies
    pkg-config
    openssl
    zlib
    # For Python development
    python312Packages.virtualenv
  ];

  shellHook = ''
    # Add target/release to PATH (for dora binaries)
    export PATH="$PWD/target/release:$PATH"
    
    # Set up Python environment variables
    export PYTHONPATH="$PWD:$PYTHONPATH"
    
    # Rust/Cargo environment
    export CARGO_HOME="$HOME/.cargo"
    export RUST_BACKTRACE=1
    
    # UV environment setup
    export UV_PYTHON="${pkgs.python312}/bin/python"
    export UV_PYTHON_DOWNLOADS="never"
    
    # C++ standard library and zlib
    export LD_LIBRARY_PATH="${pkgs.gcc.cc.lib}/lib:${pkgs.stdenv.cc.cc.lib}/lib:${pkgs.zlib}/lib:\''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
        
    # Python bindings build helper function
    build_dora_bindings() {
      cd apis/python/node
      cargo build
    }
    
    # Activate UV virtual environment if it exists
    if [ -f .venv/bin/activate ]; then
      source .venv/bin/activate
      echo "Activated virtual environment"
      # Put the system uv and python in front of the virtual environment binaries
      export PATH="${pkgs.uv}/bin:$PATH"
    else
      echo "No virtual environment found"
    fi
    
    export PS1="\[\033[1;35m\][dora:\w]\$ \[\033[0m\]"
  '';
}
