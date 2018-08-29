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

...

## == != > < and or not etc

{
    "must":{"term":{"username":"cj"}} # and ==
    "must_not":{"term":{"username":"cj"}} #and !=
    "should":[
        {"term":{"username":"cj"}},
        {"term":{"username":"cj2"},
        {"range":{"age":{"gt":"18"}}}}
    ]       # or
    "should":{
        "wildcard":{"username":"*cj*"} # like(not analyzed)
    }
}

...

## group by

{
  "aggs": {
    "group_by_state": {
      "terms": {
        "field": "state.keyword"
      }
    }
  }
}
OR
{
  "aggs": {
    "state": {
      "terms": {
        "field": "state"
      },
      "aggs": {
        "name":{
            "terms": {
                "field":"name"
            }
        }
      }
    }
  }
}

...

## having

{
    "bucket_selector":{
        "buckets_path":{
            "val_100": "count"
        },
        "script": "(val_100 == 6862)"
    }
}
