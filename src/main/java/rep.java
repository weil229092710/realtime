import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class rep extends str{

    static String a1="curl -X POST -H \"Authorization:Basic QURNSU46S1lMSU4=\" -H \"Content-Type:application/json\" -d '{\"modelDescData\":\"";
    static String  a2="\",\"cubeName\":\"fact_appservice_installed_lv_detail_cube\",\"project\":\"appservice_installed_lv\"}' http://172.16.31.2:7070/kylin/api/cubes";
    public static void main(String[] args) {

        String aa="{\n" +
                "  \"uuid\": \"97eb79d4-c14a-4394-8f1d-e837fa698056\",\n" +
                "  \"last_modified\": 1564648643814,\n" +
                "  \"version\": \"2.4.0.20500\",\n" +
                "  \"name\": \"fact_appservice_installed_lv_detail_cube\",\n" +
                "  \"is_draft\": false,\n" +
                "  \"model_name\": \"fact_appservice_installed_lv_detail\",\n" +
                "  \"description\": \"\",\n" +
                "  \"null_string\": null,\n" +
                "  \"dimensions\": [\n" +
                "    {\n" +
                "      \"name\": \"PACKAGE_NAME\",\n" +
                "      \"table\": \"FACT_APPSERVICE_INSTALLED_LV_DETAIL\",\n" +
                "      \"column\": \"PACKAGE_NAME\",\n" +
                "      \"derived\": null\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"CREATE_DATE\",\n" +
                "      \"table\": \"FACT_APPSERVICE_INSTALLED_LV_DETAIL\",\n" +
                "      \"column\": \"CREATE_DATE\",\n" +
                "      \"derived\": null\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"SRC_FILE_DAY\",\n" +
                "      \"table\": \"FACT_APPSERVICE_INSTALLED_LV_DETAIL\",\n" +
                "      \"column\": \"SRC_FILE_DAY\",\n" +
                "      \"derived\": null\n" +
                "    }\n" +
                "  ],\n" +
                "  \"measures\": [\n" +
                "    {\n" +
                "      \"name\": \"_COUNT_\",\n" +
                "      \"function\": {\n" +
                "        \"expression\": \"COUNT\",\n" +
                "        \"parameter\": {\n" +
                "          \"type\": \"constant\",\n" +
                "          \"value\": \"1\"\n" +
                "        },\n" +
                "        \"returntype\": \"bigint\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"IS_FINISH_INSTALL\",\n" +
                "      \"function\": {\n" +
                "        \"expression\": \"SUM\",\n" +
                "        \"parameter\": {\n" +
                "          \"type\": \"column\",\n" +
                "          \"value\": \"FACT_APPSERVICE_INSTALLED_LV_DETAIL.IS_FINISH_INSTALL\"\n" +
                "        },\n" +
                "        \"returntype\": \"bigint\"\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  \"dictionaries\": [],\n" +
                "  \"rowkey\": {\n" +
                "    \"rowkey_columns\": [\n" +
                "      {\n" +
                "        \"column\": \"FACT_APPSERVICE_INSTALLED_LV_DETAIL.PACKAGE_NAME\",\n" +
                "        \"encoding\": \"dict\",\n" +
                "        \"encoding_version\": 1,\n" +
                "        \"isShardBy\": false\n" +
                "      },\n" +
                "      {\n" +
                "        \"column\": \"FACT_APPSERVICE_INSTALLED_LV_DETAIL.CREATE_DATE\",\n" +
                "        \"encoding\": \"dict\",\n" +
                "        \"encoding_version\": 1,\n" +
                "        \"isShardBy\": false\n" +
                "      },\n" +
                "      {\n" +
                "        \"column\": \"FACT_APPSERVICE_INSTALLED_LV_DETAIL.SRC_FILE_DAY\",\n" +
                "        \"encoding\": \"dict\",\n" +
                "        \"encoding_version\": 1,\n" +
                "        \"isShardBy\": false\n" +
                "      }\n" +
                "    ]\n" +
                "  },\n" +
                "  \"hbase_mapping\": {\n" +
                "    \"column_family\": [\n" +
                "      {\n" +
                "        \"name\": \"F1\",\n" +
                "        \"columns\": [\n" +
                "          {\n" +
                "            \"qualifier\": \"M\",\n" +
                "            \"measure_refs\": [\n" +
                "              \"_COUNT_\",\n" +
                "              \"IS_FINISH_INSTALL\"\n" +
                "            ]\n" +
                "          }\n" +
                "        ]\n" +
                "      }\n" +
                "    ]\n" +
                "  },\n" +
                "  \"aggregation_groups\": [\n" +
                "    {\n" +
                "      \"includes\": [\n" +
                "        \"FACT_APPSERVICE_INSTALLED_LV_DETAIL.PACKAGE_NAME\",\n" +
                "        \"FACT_APPSERVICE_INSTALLED_LV_DETAIL.CREATE_DATE\",\n" +
                "        \"FACT_APPSERVICE_INSTALLED_LV_DETAIL.SRC_FILE_DAY\"\n" +
                "      ],\n" +
                "      \"select_rule\": {\n" +
                "        \"hierarchy_dims\": [],\n" +
                "        \"mandatory_dims\": [\n" +
                "          \"FACT_APPSERVICE_INSTALLED_LV_DETAIL.PACKAGE_NAME\",\n" +
                "          \"FACT_APPSERVICE_INSTALLED_LV_DETAIL.CREATE_DATE\",\n" +
                "          \"FACT_APPSERVICE_INSTALLED_LV_DETAIL.SRC_FILE_DAY\"\n" +
                "        ],\n" +
                "        \"joint_dims\": []\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  \"signature\": \"lHzCt8rRuxkymO0BU/8NgA==\",\n" +
                "  \"notify_list\": [],\n" +
                "  \"status_need_notify\": [\n" +
                "    \"ERROR\",\n" +
                "    \"DISCARDED\",\n" +
                "    \"SUCCEED\"\n" +
                "  ],\n" +
                "  \"partition_date_start\": 0,\n" +
                "  \"partition_date_end\": 3153600000000,\n" +
                "  \"auto_merge_time_ranges\": [\n" +
                "    604800000,\n" +
                "    2419200000\n" +
                "  ],\n" +
                "  \"volatile_range\": 0,\n" +
                "  \"retention_range\": 0,\n" +
                "  \"engine_type\": 2,\n" +
                "  \"storage_type\": 2,\n" +
                "  \"override_kylin_properties\": {},\n" +
                "  \"cuboid_black_list\": [],\n" +
                "  \"parent_forward\": 3,\n" +
                "  \"mandatory_dimension_set_list\": [],\n" +
                "  \"snapshot_table_desc_list\": []\n" +
                "}";
        JSONObject jsonObject =  JSON.parseObject(aa);
        jsonObject.remove("uuid");
        jsonObject.remove("last_modified");
        jsonObject.remove("version");
        jsonObject.get("name");
        System.out.println(jsonObject.get("cubeName"));
        String a= jsonObject.toString().replaceAll("\"", "\\\\\"");
        aa=  a.replaceAll("\r|\n", "");
        aa=a1+aa+a2;
        System.out.println(aa);


    }


}
