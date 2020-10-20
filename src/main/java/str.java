public class str {

    public static void main(String[] args) {

        String aa="{\n" +
                "  \"uuid\": \"e6af8a5b-ed33-4fa5-8769-a87e7db27904\",\n" +
                "  \"last_modified\": 1594796578770,\n" +
                "  \"version\": \"2.4.0.20500\",\n" +
                "  \"name\": \"fact_hear_active_teacher_daily_model\",\n" +
                "  \"owner\": \"ADMIN\",\n" +
                "  \"is_draft\": false,\n" +
                "  \"description\": \"\",\n" +
                "  \"fact_table\": \"RPTDATA.FACT_HEAR_ACTIVE_TEACHER_DAILY\",\n" +
                "  \"lookups\": [],\n" +
                "  \"dimensions\": [\n" +
                "    {\n" +
                "      \"table\": \"FACT_HEAR_ACTIVE_TEACHER_DAILY\",\n" +
                "      \"columns\": [\n" +
                "        \"SCHOOL_ID\",\n" +
                "        \"SCHOOL_NAME\",\n" +
                "        \"SRC_FILE_DAY\",\n" +
                "        \"WEEK_DAY\",\n" +
                "        \"USER_TYPE\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"metrics\": [\n" +
                "    \"FACT_HEAR_ACTIVE_TEACHER_DAILY.USER_ID\",\n" +
                "    \"FACT_HEAR_ACTIVE_TEACHER_DAILY.ACTIVE_USER_ID\"\n" +
                "  ],\n" +
                "  \"filter_condition\": \"\",\n" +
                "  \"partition_desc\": {\n" +
                "    \"partition_date_column\": \"FACT_HEAR_ACTIVE_TEACHER_DAILY.SRC_FILE_DAY\",\n" +
                "    \"partition_time_column\": null,\n" +
                "    \"partition_date_start\": 0,\n" +
                "    \"partition_date_format\": \"yyyyMMdd\",\n" +
                "    \"partition_time_format\": \"HH:mm:ss\",\n" +
                "    \"partition_type\": \"APPEND\",\n" +
                "    \"partition_condition_builder\": \"org.apache.kylin.metadata.model.PartitionDesc$DefaultPartitionConditionBuilder\"\n" +
                "  },\n" +
                "  \"capacity\": \"MEDIUM\"\n" +
                "}";
        String aaa=  aa.replaceAll("\"", "\\\\\"");
        System.out.println(aaa);
    }


}
