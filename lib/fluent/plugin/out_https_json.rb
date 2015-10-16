class Fluent::HttpsJsonOutput < Fluent::TimeSlicedOutput
  # First, register the plugin. NAME is the name of this plugin
  # and identifies the plugin in the configuration file.
  Fluent::Plugin.register_output('https_json', self)

  def initialize
    require 'net/http/persistent'
    require 'uri'
    super
  end

  # This method is called before starting.
  # 'conf' is a Hash that includes configuration parameters.
  # If the configuration is invalid, raise Fluent::ConfigError.
  def configure(conf)
    super
    @uri = URI conf['endpoint']

    if conf['remove_prefix']
      @remove_prefix = conf['remove_prefix']
      @remove_prefix_string = @remove_prefix + '.'
      @remove_prefix_length = @remove_prefix_string.length
    end
  end

  # This method is called when starting.
  # Open sockets or files here.
  def start
    super
    @http = Net::HTTP::Persistent.new()
  end

  # This method is called when shutting down.
  # Shutdown the thread and close sockets or files here.
  def shutdown
    super
    @http.shutdown
  end

  ## Optionally, you can use to_msgpack to serialize the object.
  def format(tag, time, record)
    if tag == @remove_prefix or @remove_prefix and (tag[0, @remove_prefix_length] == @remove_prefix_string and tag.length > @remove_prefix_length)
      tag = tag[@remove_prefix_length..-1]
    end 
    [tag, time, record].to_msgpack
  end

  # This method is called every flush interval. Write the buffer chunk
  # to files or databases here.
  # 'chunk' is a buffer chunk that includes multiple formatted
  # events. You can use 'data = chunk.read' to get all events and
  # 'chunk.open {|io| ... }' to get IO objects.
  def write(chunk)
    events = []
    chunk.msgpack_each {|(tag,time,record)|
      events << {:tag => tag, :time => time, :record => record}
    }
    events = events.to_json
    req = Net::HTTP::Post.new(@uri.path)
    req.set_form_data({"events" => events})
    res = @http.request(@uri, req)
  end

end
