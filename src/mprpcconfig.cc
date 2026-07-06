#include "mprpcconfig.h"
#include <iostream>
#include <string>
#include <cstring>
//负责加载配置文件
void MprpcConfig::LoadConfigFile(const char *config_file){
    FILE *pf = fopen(config_file, "r");
    if(pf == nullptr){
        std::cout << config_file << "does not exist!" << std::endl;
        exit(EXIT_FAILURE);
    }


    // 1. 注释 2. 正确的配置项 = 3. 去掉开头多余的空格
    while(!feof(pf)){
        char buf[512] = {0};
        if(fgets(buf, 512, pf) != nullptr){
            buf[strcspn(buf, "\n")] = '\0';
        }
        // 去掉字符串前面多余的空格
        std::string read_buf(buf);
        trim(read_buf);

        // 处理注释/空行
        if(read_buf.empty() || read_buf[0] == '#'){
            continue;
        }
        // 解析配置项
        int idx = read_buf.find('=');
        if(idx == -1){
            //配置项不合法
            continue;
        }
        std::string key;
        std::string value;
        key = read_buf.substr(0, idx);
        trim(key);
        value = read_buf.substr(idx + 1, read_buf.size() - idx);
        trim(value);  
        m_configMap.insert({key, value});
    }
    fclose(pf);
}
//查询配置项信息
std::string MprpcConfig::Load(const std::string &key){
    auto it = m_configMap.find(key);
    if(it == m_configMap.end()){
        return "";
    }
    return it->second;
}

void MprpcConfig::trim(std::string &src_buf){
    int idx = src_buf.find_first_not_of(' ');
    if(idx != -1){
        src_buf = src_buf.substr(idx, src_buf.size() - idx);
    }

    // 去掉字符串后面面多余的空格
    idx = src_buf.find_last_not_of(' ');
    if(idx != -1){
        src_buf = src_buf.substr(0, idx + 1);
    }

}