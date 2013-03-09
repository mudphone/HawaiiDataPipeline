#!/usr/bin/env ruby -w
require 'net/https'
require 'json'
require 'set'

module HGovData
  API_URL = "data.hawaii.gov"
  
  class Client

    def initialize(opts = {:app_token => "K6rLY8NBK0Hgm8QQybFmwIUQw"})
      @config = {}
      @config.merge(opts)
    end

    def response_for! url
      # Create our request
      use_ssl = url.start_with? "https://"
      uri = URI.parse(url)
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = use_ssl
      if use_ssl
        http.verify_mode = OpenSSL::SSL::VERIFY_NONE
      end

      request = Net::HTTP::Get.new(uri.request_uri)
      request.add_field("X-App-Token", @config[:app_token])
      response = http.request request
      { body: response.body,
        code: response.code }
    end

    def response_for url
      @expensive_request ||= {}
      @expensive_request[url] ||= response_for!(url)
    end
    
    # assumes json, http (not https)
    def get! url
      response = response_for! url
      # Check our response code
      if response[:code] != "200"
        raise "Error querying \"#{uri.to_s}\": #{response.body}"
      else
        return response[:body]
      end
    end

    def get url
      @expensive_get ||= {}
      @expensive_get[url] ||= get!(url)
    end

    def get_json url
      return JSON.parse(get(url))
    end

    def clear_cache!
      get_size = @expensive_get ? @expensive_get.size : 0
      @expensive_get = {}
      puts "Cache of #{get_size} URL#{get_size == 1 ? '' : 's'} cleared."

      request_size = @expensive_request ? @expensive_request.size : 0
      @expensive_request = {}
      puts "Cache of #{request_size} request#{request_size == 1 ? '' : 's'} cleared."
      
      dataset_size = @dataset_links ? @dataset_links.size : 0
      @dataset_links = nil
      puts "Cache of #{dataset_size} dataset name#{dataset_size == 1 ? '' : 's'} cleared."
      
    end

    # client.views                 # returns all views, all columns
    # client.views(limit: 2)       # limits returned dataset to 2
    # client.views(cols: [:name])  # returns only the "name" kv pair, for all views
    # client.views(limit: 3, cols: [:id, :name])  # returns the "id" and "name" kv pairs, 
                                                  #   limiting to three views
    def views(opts={})
      limit = opts[:limit]
      cols = opts[:cols] || []

      url = "http://#{API_URL}/api/views"
      url += "?limit=#{limit}" if limit
      all_views = get_json url
      
      return all_views if cols.empty?

      column_names = cols.map{ |c| c.to_s }
      all_views.map do |v| 
        v.reject! { |k, v| !column_names.include?(k) }
      end
      return all_views
    end

    # Sorted by a key
    def views_sorted_by sort_thing
      views.sort_by{ |v| v[sort_thing.to_s] }
    end

    # All keys in a view
    def view_keys
      sample = views(limit: 1)
      return nil if sample.empty?
      sample.first.keys.map { |k| k.to_sym }
    end

    # List of all dataset view names
    def list_views
      views_sorted_by('name').each do |n|
        puts "#{n['name']}"
      end
      nil
    end

    def data_for id
      get_json "http://#{API_URL}/resource/#{id}.json"
    end

    def datasets
      return @dataset_links unless @dataset_links.nil?
      
      links = Set.new
      1.upto(100) do |n|
        puts "Looking for datasets on page #: #{n}"
        url = "https://#{API_URL}/browse/embed?limitTo=datasets&page=#{n}"
        puts "url is: #{url}"
        response = response_for url
        break if response[:code] != "200"
        body = response[:body]
        new_links = body.scan(/href="(?:http:\/\/.*?)?(\/[^\/]*?\/[^\/]*?)\/(.{4,4}-.{4,4})"/)
        break if new_links.empty?
        links.merge Set.new(new_links)
        puts "... #{links.size} unique datasets found... (still searching)"
      end
      puts "Search complete, found #{links.size} datasets."
      @dataset_links = links
    end

    def list_datasets
      sorted_datasets.each_with_index do |d, idx|
        puts "#{idx}) Name: #{d.first}  ID: #{d[1]}"
      end
      nil
    end

    def sorted_datasets
      datasets.to_a.sort_by { |ds| ds.first }
    end
  end
end
