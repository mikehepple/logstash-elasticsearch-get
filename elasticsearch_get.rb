require "logstash/filters/base"
require "logstash/namespace"
require "logstash/util/fieldreference"


#
# Fetch an elasticsearch document and add the specified fields to this
# event. This can be useful if you want to enrich log messages, e.g. auditing, 
# with additional data
#
# Example usage:
# elasticsearch_get {
#	hosts => ["localhost"]
#	id => "1"
#	index => "blog"
#	doc_type => "entry"
#	fields => [	
#		"entry","blog_entry",
#		"author","blog_author",
#	]
# }
#
class LogStash::Filters::ElasticsearchGet < LogStash::Filters::Base
  config_name "elasticsearch_get"
  milestone 1

  # List of elasticsearch hosts to use for querying.
  config :hosts, :validate => :array

  # Elasticsearch query string
  config :id, :validate => :string

  # Hash of fields to copy from old event (found via elasticsearch) into new event
  config :fields, :validate => :hash, :default => {}

  config :index, :validate => :string
  config :doc_type, :validate => :string

  public
  def register
    require "elasticsearch"

    @logger.info("New ElasticSearch filter", :hosts => @hosts)
    @client = Elasticsearch::Client.new hosts: @hosts
  end # def register

  public
  def filter(event)
    
    return unless filter?(event)
    @logger.info("Starting elasticsearch filter", :index => event.sprintf(@index), :doc_type => event.sprintf(@doc_type), :id => event.sprintf(@id), :exists => filter?(event))

    begin
      id_str = event.sprintf(@id)
      index_str = event.sprintf(@index)
      type_str = event.sprintf(@doc_type)
      @logger.info("Fetching document", :index => index_str, :type => type, :id => id)
      results = @client.get id: id_str, index: index_str, type: type_str
      found = results['found']
      @logger.info("Fetch completed", :found => found)
      @logger.debug("Fetch completed", :results => results)

      if found
	      @fields.each do |old, new|
		event[new] = results['_source'][old]
	      end

	      filter_matched(event)
      end
    rescue => e
      @logger.warn("Failed to query elasticsearch for previous event",
                   :error => e)
    end
  end # def filter
end # class LogStash::Filters::Elasticsearch
