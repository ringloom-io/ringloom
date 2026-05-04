{
  "targets": [
    {
      "target_name": "ringloom_node",
      "sources": [
        "src/addon.cc"
      ],
      "include_dirs": [
        "../../include",
        "<!@(node -p \"require('node-addon-api').include\")"
      ],
      "dependencies": [
        "<!(node -p \"require('node-addon-api').gyp\")"
      ],
      "defines": [
        "NAPI_DISABLE_CPP_EXCEPTIONS"
      ],
      "cflags_cc": [
        "-std=c++17"
      ],
      "conditions": [
        [
          "OS==\"linux\"",
          {
            "libraries": [
              "-L<(module_root_dir)/../../zig-out/lib",
              "-lringloom_service"
            ],
            "ldflags": [
              "-Wl,-rpath,'$$ORIGIN'"
            ],
            "copies": [
              {
                "destination": "<(PRODUCT_DIR)",
                "files": [
                  "<(module_root_dir)/../../zig-out/lib/libringloom_service.so"
                ]
              }
            ]
          }
        ],
        [
          "OS==\"mac\"",
          {
            "libraries": [
              "<(module_root_dir)/../../zig-out/lib/libringloom_service.dylib"
            ],
            "xcode_settings": {
              "CLANG_CXX_LANGUAGE_STANDARD": "c++17",
              "MACOSX_DEPLOYMENT_TARGET": "13.0",
              "OTHER_LDFLAGS": [
                "-Wl,-rpath,@loader_path"
              ]
            },
            "copies": [
              {
                "destination": "<(PRODUCT_DIR)",
                "files": [
                  "<(module_root_dir)/../../zig-out/lib/libringloom_service.dylib"
                ]
              }
            ]
          }
        ]
      ]
    }
  ]
}
