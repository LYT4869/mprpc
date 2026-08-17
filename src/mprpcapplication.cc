#include "mprpcapplication.h"
#include <iostream>
#include <string>

MprpcConfig MprpcApplication::m_config;

void ShowArgsHelp(){
    std::cout<< "format: command -i <configfile>" << std::endl;
}
void MprpcApplication::Init(int argc, char **argv){
    if(argc < 2){
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }

    std::string config_file;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "-i") {
            if (i + 1 >= argc) {
                ShowArgsHelp();
                exit(EXIT_FAILURE);
            }
            config_file = argv[++i];
        }
    }

    if (config_file.empty()) {
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }

    // 加载配置文件
    m_config.LoadConfigFile(config_file.c_str());
}

MprpcApplication& MprpcApplication::GetInstance(){
    static MprpcApplication app;
    return app;
}

MprpcConfig& MprpcApplication::GetConfig(){
    return m_config;
}
