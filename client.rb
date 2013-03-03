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

    def views
      get "http://data.honolulu.gov/api/views"
    end

    def list
      views.sort_by{ |v| v['name'] }.each do |n|
        puts "#{n['name']}"
      end
      nil
    end

  end
end