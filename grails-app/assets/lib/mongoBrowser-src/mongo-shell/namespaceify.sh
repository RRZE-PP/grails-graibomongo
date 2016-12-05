#!/bin/bash

FORCE=$1
FILENAME=$2
if [[ "$FORCE" != "-f" ]]; then
    FILENAME=$1
    FORCE=""
fi

if [[ "$FILENAME" = "" ]]; then
    echo "Usage: $0 [-f] FILENAME"
    exit
fi

if [[ -e "$FILENAME" && "$FORCE" != "-f" ]]; then
    while true; do
        read -p "File exists. Overwrite? (Y/n)" yn
        case $yn in
        "" )    break;;
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * )     echo "Please answer y or n.";;
        esac
    done
fi

sed -e '/\/\* *BEGIN_INSERT/,$d' namespaceTemplate.js > "$FILENAME"

echo "// ---- MODULE: assert ---- " >> "$FILENAME";        cat assert.js >> "$FILENAME";        echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: bson ---- " >> "$FILENAME";          cat bson.js >> "$FILENAME";          echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: error_codes ---- " >> "$FILENAME";   cat error_codes.js >> "$FILENAME";   echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: port_bson ---- " >> "$FILENAME";     cat port_bson.js >> "$FILENAME";     echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: port_misc ---- " >> "$FILENAME";     cat port_misc.js >> "$FILENAME";     echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: port_cursor ---- " >> "$FILENAME";   cat port_cursor.js >> "$FILENAME";   echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: collection ---- " >> "$FILENAME";    cat collection.js >> "$FILENAME";    echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: bridge ---- " >> "$FILENAME";        cat bridge.js >> "$FILENAME";        echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: bulk_api ---- " >> "$FILENAME";      cat bulk_api.js >> "$FILENAME";      echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: crud_api ---- " >> "$FILENAME";      cat crud_api.js >> "$FILENAME";      echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: port_db ---- " >> "$FILENAME";       cat port_db.js >> "$FILENAME";       echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: db ---- " >> "$FILENAME";            cat db.js >> "$FILENAME";            echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: explainable ---- " >> "$FILENAME";   cat explainable.js >> "$FILENAME";   echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: explain_query ---- " >> "$FILENAME"; cat explain_query.js >> "$FILENAME"; echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: mongo ---- " >> "$FILENAME";         cat mongo.js >> "$FILENAME";         echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: port_mongo ---- " >> "$FILENAME";    cat port_mongo.js >> "$FILENAME";    echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: port_rpc ---- " >> "$FILENAME";      cat port_rpc.js >> "$FILENAME";      echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: mr ---- " >> "$FILENAME";            cat mr.js >> "$FILENAME";            echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: query ---- " >> "$FILENAME";         cat query.js >> "$FILENAME";         echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: replsettest ---- " >> "$FILENAME";   cat replsettest.js >> "$FILENAME";   echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: servers ---- " >> "$FILENAME";       cat servers.js >> "$FILENAME";       echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: servers_misc ---- " >> "$FILENAME";  cat servers_misc.js >> "$FILENAME";  echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: shardingtest ---- " >> "$FILENAME";  cat shardingtest.js >> "$FILENAME";  echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: types ---- " >> "$FILENAME";         cat types.js >> "$FILENAME";         echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: upgrade_check ---- " >> "$FILENAME"; cat upgrade_check.js >> "$FILENAME"; echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: utils_auth ---- " >> "$FILENAME";    cat utils_auth.js >> "$FILENAME";    echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: utils ---- " >> "$FILENAME";         cat utils.js >> "$FILENAME";         echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: utils_sh ---- " >> "$FILENAME";      cat utils_sh.js >> "$FILENAME";      echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: port_connection ---- " >> "$FILENAME";    cat port_connection.js >> "$FILENAME";   echo -e "\n\n" >> "$FILENAME"
echo "// ---- MODULE: codemirror_hinter ---- " >> "$FILENAME";  cat codemirror_hinter.js >> "$FILENAME"; echo -e "\n\n" >> "$FILENAME"

sed -n -e '/\/\* *END_INSERT/,${//!p;}' namespaceTemplate.js >> "$FILENAME"
