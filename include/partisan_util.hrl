-define(RECORD_TO_LIST(Record),
    lists:zip(?MODULE:record_info(fields, Record), tl(tuple_to_list(Record)))
).

-define(RECORD_TO_MAP(Record),
    maps:from_list(?RECORD_TO_LIST(Record))
).