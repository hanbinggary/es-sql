# some elasticsearch dsl  <=> sql

## limit 0,10

{
    "from":0,"size":10
    #defalut 10,max index.max_result_window=10000
}

...

## order by name desc,age asc

{
    "sort" : [
        { "name" : "desc" },
        { "age" : "asc" },
    ]
}

The order defaults to desc when sorting on the _score, and defaults to asc when sorting on anything else.

...

## select username from ...

{
    "_source": ["username"]
}

...

## scroll

url : index_name/_search?scroll=1m
{
    "size": 3, # max return
    "_source": [
        "info_time"
    ],
    "sort": [
        "info_time"
    ]
}

url: _search/scroll
{
    "scroll" : "1m",
    "scroll_id" : "xxxx"
}