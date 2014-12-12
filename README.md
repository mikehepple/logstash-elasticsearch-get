logstash-elasticsearch-get
==========================

logstash plugin to fetch a document from an elasticsearch instance and append the specified fields to the event.

Example usage
=============

```ruby
elasticsearch_get {
      hosts => ["localhost"]
      id => "1"
      index => "blog"
      doc_type => "entry"
      fields => [     
              "entry","blog_entry",
              "author","blog_author",
      ]
}
```
