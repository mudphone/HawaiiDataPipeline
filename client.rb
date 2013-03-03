#!/usr/bin/env ruby -w
require 'net/https'
require 'json'

module HGovData
  class Client

    def initialize(opts = {:app_token => "K6rLY8NBK0Hgm8QQybFmwIUQw"})
      @config = {}
      @config.merge(opts)
    end

    def get(url)
      # assumes json, http (not https)

      # Create our request
      uri = URI.parse(url)
      http = Net::HTTP.new(uri.host, uri.port)
      # http.use_ssl = true

      request = Net::HTTP::Get.new(uri.request_uri)
      request.add_field("X-App-Token", @config[:app_token])
      
      # BAM!
      response = http.request(request)

      # Check our response code
      if response.code != "200"
        raise "Error querying \"#{uri.to_s}\": #{response.body}"
      else
        # return Hashie::Mash.new(response.body)
        return JSON.parse(response.body)
      end
    end

    # client.views                 # returns all views, all columns
    # client.views(limit: 2)       # limits returned dataset to 2
    # client.views(cols: [:name])  # returns only the "name" kv pair, for all views
    # client.views(limit: 3, cols: [:id, :name])  # returns the "id" and "name" kv pairs, 
                                                  #   limiting to three views
    def views(opts={})
      limit = opts[:limit]
      cols = opts[:cols] || []

      url = "http://data.honolulu.gov/api/views"
      url += "?limit=#{limit}" if limit
      all_views = get url
      
      return all_views if cols.empty?

      column_names = cols.map{ |c| c.to_s }
      all_views.map do |v| 
        v.reject! { |k, v| !column_names.include?(k) }
      end
      return all_views
    end

    # 
    def views_sorted_by sort_thing
      views.sort_by{ |v| v[sort_thing.to_s] }
    end

    def view_keys
      sample = views(limit: 1)
      return nil if sample.empty?
      sample.first.keys.map { |k| k.to_sym }
    end

    def list
      views_sorted_by('name').each do |n|
        puts "#{n['name']}"
      end
      nil
    end

    def data(id)
      get "http://data.honolulu.gov/resource/#{id}.json"
    end

  end
end