require 'roo'
require 'net/http'
require 'uri'
require 'json'

class AnalysisController < ApplicationController
  before_action :load_file_data, only: :run
  skip_before_action :verify_authenticity_token, only: [:stop]
  @@progress = { current: 0, total: 0 }
  @@stop_flag = false 
  LYTX_SLT_URL = "https://api.lytx.com/v1/authenticate/token"

  def self.progress
    @@progress
  end
  def self.progress=(value)
    @@progress = value
  end

  def progress
    self.class.logger.silence do
      report_ready = Rails.cache.read("report_path_#{session.id}").present?
      report_name  = Rails.cache.read("report_name_#{session.id}")
      render json: AnalysisController.progress.merge(
        stopped: AnalysisController.stop_flag,
        error: AnalysisController.progress[:error],
        last_row: AnalysisController.progress[:last_row],
        report_ready: report_ready,
        report_name: report_name
      )
    end
  end

  def stop
    AnalysisController.stop_flag = true
    AnalysisController.progress = { current: 0, total: 0, last_row: AnalysisController.progress[:last_row] }
    #puts "Shutting down process"
    render json: { message: "Analysis will stop shortly." }
  end

  def self.stop_flag
    @@stop_flag
  end

  def self.stop_flag=(value)
    @@stop_flag = value
  end

  # Returns a valid Lytx SLT from cache (refreshes if missing/near expiry)
  def lytx_bearer_token
    tok = Rails.cache.read("lytx_slt_token")
    exp = Rails.cache.read("lytx_slt_expires_at")

    # refresh if missing or expiring within 60s
    if tok.blank? || exp.blank? || Time.current >= (exp - 60)
      fetch_lytx_short_lived_token_to_cache!
      tok = Rails.cache.read("lytx_slt_token")
    end

    tok
  end


  # Upload page
  def index
  end

  # Upload file & store data in session (or temp file)
  def import
    if params[:file].blank?
      redirect_to analysis_path, alert: "Please select a file"
      return
    end
  
    # Save uploaded file to persistent storage (not /tmp)
    uploaded_file = params[:file]
    uploads_dir = Rails.root.join("storage", "uploads")
    FileUtils.mkdir_p(uploads_dir)
    saved_path = uploads_dir.join(uploaded_file.original_filename)

    File.open(saved_path, "wb") { |f| f.write(uploaded_file.read) }

    # Store path both in session and server-side cache for reliability
    session[:gas_file_path] = saved_path.to_s
    Rails.cache.write("upload_path_#{session.id}", saved_path.to_s, expires_in: 12.hours)

    Rails.logger.info "[analysis] uploaded file saved to #{saved_path} (#{File.size(saved_path)} bytes) for session=#{session.id}"

    redirect_to analysis_path, notice: "File uploaded. Click 'Analyze' to start."

  end

  # Run analysis
  def run
    file_path = Rails.cache.read("upload_path_#{session.id}") || session[:gas_file_path]

    unless file_path && File.exist?(file_path)
      Rails.logger.warn "[analysis] uploaded file missing for session=#{session.id.inspect} path=#{file_path.inspect}"
      redirect_to analysis_path, alert: "No uploaded file found on server. Please upload again."
      return
    end
    Rails.logger.info "[analysis] starting with file present: #{file_path} (#{File.size(file_path)} bytes)"
    

    begin
      fetch_lytx_short_lived_token_to_cache!
    rescue => e
      redirect_to analysis_path, alert: "Could not get Lytx token: #{e.message}"
      return
    end
  
    # Reset stop flag
    AnalysisController.stop_flag = false
    sid = session.id

    # Start the analysis in a background thread (with proper logging/executor)
    if params[:sync].to_s == "1" || ENV["SYNC_ANALYSIS"] == "1"
      Rails.logger.info "[analysis] SYNC mode enabled – running perform_analysis inline"
      begin
        Rails.application.executor.wrap do
          perform_analysis(file_path, params[:row_limit], params[:row_offset], sid)
        end
        Rails.logger.info "[analysis] SYNC perform_analysis finished"
      rescue => e
        Rails.logger.error("[analysis] SYNC perform_analysis crashed: #{e.class}: #{e.message}\n#{e.backtrace&.join("\n")}")
        AnalysisController.progress[:error] = true
      end
    else
      # Start the analysis in a background thread (with proper logging/executor)
      thr = Thread.new do
        Thread.current.name = "analysis-#{sid}" rescue nil
        Thread.current.abort_on_exception = true
        Thread.report_on_exception = true rescue nil
    
        Rails.logger.info "[analysis] thread started for session=#{sid} file=#{file_path}"
        begin
          Rails.application.executor.wrap do
            perform_analysis(file_path, params[:row_limit], params[:row_offset], sid)
          end
        rescue => e
          Rails.logger.error("[analysis] thread crashed: #{e.class}: #{e.message}\n#{e.backtrace&.join("\n")}")
          AnalysisController.progress[:error] = true
        ensure
          AnalysisController.stop_flag = false
          Rails.logger.info "[analysis] thread finished for session=#{sid}"
        end
      end
      Rails.logger.info "[analysis] thread created: #{thr.inspect}"
    end
     

  
    # Immediately return so UI can poll progress and send stop
    redirect_to analysis_path, notice: "Analysis started. Progress will update automatically."
  end


  def generate_report
    file_path = Rails.cache.read("report_path_#{session.id}")
    filename  = Rails.cache.read("report_name_#{session.id}")
    return redirect_to analysis_path, alert: "No report found. Please run analysis first." unless file_path && File.exist?(file_path)
  
    # Load data from file
    report_data = JSON.parse(File.read(file_path), symbolize_names: true)
  
    package = Axlsx::Package.new
    # Excel worksheet name must be <= 31 chars and cannot contain : \ / ? * [ ]
    used_sheet_names = {}
    excel_sheet_name = ->(name) do
      base = name.to_s.gsub(%r{[:\\/\?\*\[\]]}, "")
      base = base[0, 31]
      n = base
      i = 1
      while used_sheet_names[n]
        suffix = " (#{i})"
        n = base[0, 31 - suffix.length] + suffix
        i += 1
      end
      used_sheet_names[n] = true
      n
    end
    wb = package.workbook
  
    add_sheet = ->(title, rows) do
      safe_title = excel_sheet_name.call(title)
      wb.add_worksheet(name: safe_title) do |sheet|
        next if rows.empty?
        sheet.add_row rows.first.keys
        rows.each { |row| sheet.add_row row.values }
      end
    end    
  
    passed_rows = report_data[:passed_rows] || []

    # Filter passed rows for Sunday transactions
    passed_on_sunday = passed_rows.select do |row|
      begin
        dt = Time.strptime("#{row[:date]} #{row[:time]}", "%m/%d/%y %H:%M:%S")
        dt.wday == 0 # Sunday = 0
      rescue
        false
      end
    end
    
    add_sheet.call("Missing Vehicle ID", report_data[:missing_vehicle_id_rows] || [])
    add_sheet.call("Missing Vehicle GPS", report_data[:missing_coords_rows] || [])
    add_sheet.call("Flagged Over 1000 ft", report_data[:flagged_rows] || [])    
    add_sheet.call("Valid Transactions", passed_rows)
    add_sheet.call("Sunday Transactions", passed_on_sunday) unless passed_on_sunday.empty?
    add_sheet.call("Negative Distance Traveled", report_data[:negative_distance_rows] || [])
  
    send_data package.to_stream.read,
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      filename: filename || "analysis_report.xlsx"

  end
  

  



  private








  
  # Generic GET with timeouts, retries, jitter, and Lytx 401 auto-refresh
  def http_get_with_retry(url, headers: {}, max_retries: 5, purpose: nil, open_timeout: 5, timeout: 20)
    attempt = 0
    last_status = nil
    last_body = nil

    begin
      attempt += 1
      conn = Faraday.new(request: { open_timeout: open_timeout, timeout: timeout })
      response = conn.get(url) { |req| headers.each { |k, v| req.headers[k] = v } }
      last_status = response.status
      last_body = response.body

      # If token expired at Lytx, refresh once and retry immediately
      if response.status == 401 && url.include?("lytx.com")
        fetch_lytx_short_lived_token_to_cache!
        refreshed_headers = headers.merge("Authorization" => "Bearer #{lytx_bearer_token}")
        response = conn.get(url) { |req| refreshed_headers.each { |k, v| req.headers[k] = v } }
        last_status = response.status
        last_body = response.body
      end

      # Retry on transient server errors
      if [500, 502, 503, 504].include?(response.status)
        raise "HTTP #{response.status}"
      end

      return response
    rescue => e
      if attempt <= max_retries
        sleep((2**(attempt - 1)) + rand * 0.3) # backoff + jitter
        retry
      end
      msg = +"GET #{purpose || url} failed after #{attempt - 1} retries: #{e}"
      msg << " — last status: #{last_status}" if last_status
      msg << " — last body: #{last_body}" if last_body
      raise msg
    end
  end


  
  
  # Fetch a brand-new SLT and save to Rails.cache with an expiry timestamp
  def fetch_lytx_short_lived_token_to_cache!
    signed_jwt = ENV.fetch("LYTX_SIGNED_JWT")
    uri = URI.parse(LYTX_SLT_URL)

    req = Net::HTTP::Get.new(uri)
    req["Authorization"] = "Bearer #{signed_jwt}"
    req["Accept"]        = "application/json"

    resp = Net::HTTP.start(uri.hostname, uri.port, use_ssl: true) { |h| h.request(req) }
    raise "SLT fetch failed: HTTP #{resp.code} — #{resp.body}" unless resp.is_a?(Net::HTTPSuccess)

    data   = JSON.parse(resp.body) rescue {}
    token  = data["access_token"] || data["token"] || data["bearerToken"]
    # Lytx sometimes returns expires_in seconds; default to 10 min if missing
    ttl_s  = (data["expires_in"] || data["expiresIn"] || 600).to_i
    raise "SLT fetch failed: token missing in response" if token.to_s.empty?

    Rails.cache.write("lytx_slt_token", token, expires_in: ttl_s.seconds)
    Rails.cache.write("lytx_slt_expires_at", Time.current + ttl_s.seconds, expires_in: ttl_s.seconds)
    Rails.logger.info("Fetched new Lytx SLT (cached).")
    token
  end

  

  def perform_analysis(file_path, row_limit_param, row_offset_param, sid)
    Rails.logger.info "[analysis] perform_analysis starting file=#{file_path}"
    spreadsheet = Roo::Spreadsheet.open(file_path)
    state_file = Rails.root.join("tmp", "analysis_state.json")
    headers = spreadsheet.row(1).map { |h| h.to_s.downcase.gsub(/[^\w]+/, "_").to_sym }
    results = []
    if File.exist?(state_file)
      saved_state = JSON.parse(File.read(state_file), symbolize_names: true)
      last_row = saved_state[:last_row] || 2
      missing_vehicle_id_rows = saved_state[:missing_vehicle_id_rows] || []
      missing_coords_rows      = saved_state[:missing_coords_rows]      || []
      flagged_rows             = saved_state[:flagged_rows]             || []
      passed_rows              = saved_state[:passed_rows]              || []
      negative_distance_rows   = saved_state[:negative_distance_rows]   || []
      start_row = last_row + 1
      
      # Counters restored from arrays
      missing_vehicle_id_count = missing_vehicle_id_rows.length
      missing_coords_count     = missing_coords_rows.length
      flagged_count            = flagged_rows.length
      passed_count             = passed_rows.length
      negative_distance_count  = negative_distance_rows.length      
    
      # Calculate end_row same as fresh run
      row_limit = row_limit_param.to_i
      row_limit = 10000 if row_limit <= 0
      end_row = [start_row + row_limit - 1, spreadsheet.last_row].min
      total_rows = end_row - start_row + 1
    else
      row_limit = row_limit_param.to_i
      row_offset = row_offset_param.to_i
    
      row_limit = 10000 if row_limit <= 0
      row_offset = 2 if row_offset < 2
    
      start_row = row_offset
      end_row   = [start_row + row_limit - 1, spreadsheet.last_row].min
      total_rows = end_row - start_row + 1
    
      missing_vehicle_id_rows = []
      missing_coords_rows = []
      flagged_rows = []
      passed_rows = []
      negative_distance_rows = []

      negative_distance_count = 0
      missing_vehicle_id_count = 0
      missing_coords_count = 0
      flagged_count = 0
      passed_count = 0
    end

    begin
      retries = 0
      (start_row..end_row).each_with_index do |i, idx|
        puts "Sleeping for 0.5 seconds to throttle API requests..." if idx % 100 == 0
        sleep(0.5) # Throttle to avoid overwhelming the API
        #if idx >= 50
        #  raise Net::OpenTimeout, "Simulated timeout after 50 rows"
        #end
        break if AnalysisController.stop_flag
        #puts "=" * 80
        row_data = Hash[[headers, spreadsheet.row(i)].transpose]
  
        # update progress
        AnalysisController.progress = { current: idx + 1, total: total_rows, last_row: start_row + idx }
        AnalysisController.progress[:last_row] = start_row + idx

        vin         = row_data[:vin]
        date_str    = row_data[:transaction_date]
        time_str    = row_data[:transaction_time]
    
        if date_str.blank? || time_str.blank?
          Rails.logger.warn "Skipping row: Missing date or time for VIN #{vin}"
          next
        end
    
        if date_str.is_a?(Date) || date_str.is_a?(DateTime)
          date_str = date_str.strftime("%m/%d/%y")
        end
    
        if time_str.is_a?(Numeric)
          # Convert seconds since midnight to HH:MM:SS
          hours = (time_str / 3600).to_i
          minutes = ((time_str % 3600) / 60).to_i
          seconds = (time_str % 60).to_i
          time_str = format("%02d:%02d:%02d", hours, minutes, seconds)
        end

        break if AnalysisController.stop_flag
    
        address     = row_data[:merchant_address]
        city        = row_data[:merchant_city]
        state       = row_data[:merchant_state_province]
        postal_code = row_data[:merchant_postal_code]
        driver_name = "#{row_data[:driver_first_name]} #{row_data[:driver_last_name]}"
        line_id     = row_data[:emboss_line_2]

        current_odometer = row_data[:current_odometer].to_f
        previous_odometer = row_data[:previous_odometer].to_f

        if previous_odometer > current_odometer
          puts "Row #{start_row + idx} — Negative odometer reading detected for VIN: #{vin}"
          negative_distance_count += 1
          negative_distance_rows << {
            row: start_row + idx,
            driver: driver_name,
            vehicle_id: line_id,
            vin: vin,
            date: date_str,
            time: time_str,
            previous_odometer: previous_odometer,
            current_odometer: current_odometer,
            difference: (previous_odometer - current_odometer).round(2)
          }
          next
        end
      
        # 1. Get vehicle identifier
        vehicle_id = vin_to_id_map[vin.to_s.upcase]

        if vehicle_id.nil?
          #puts "Row #{start_row + idx} — No vehicle ID found for VIN: #{vin}"
          missing_vehicle_id_count += 1
          missing_vehicle_id_rows << {
              row: start_row + idx,
              driver: driver_name,
              vehicle_id: line_id,
              vin: vin,
              date: date_str,
              time: time_str,
              gas_station_address: "#{address}, #{city}, #{state} #{postal_code}"
          }
          next
        end
    
        # 2. Get vehicle coordinates
        vehicle_cords_resp = get_cords_from_vehicle_time(vehicle_id: vehicle_id, date: date_str, time: time_str)
        vehicle_point = vehicle_cords_resp.dig(:data, 0, :points, 0)
        if !vehicle_point || !vehicle_point[:latitude] || !vehicle_point[:longitude]
          #puts "Row #{start_row + idx} — No coordinates found for vehicle ID: #{vehicle_id} at #{date_str} #{time_str}"
          missing_coords_count += 1
          missing_coords_rows << {
              row: start_row + idx,
              driver: driver_name,
              vehicle_id: line_id,
              vin: vin,
              date: date_str,
              time: time_str,
              gas_station_address: "#{address}, #{city}, #{state} #{postal_code}"
          }
          next
        end
    
        # 3. Get gas station coordinates
        station_cords = get_cords_from_address(
          merchant_address: address,
          merchant_city: city,
          merchant_state: state,
          merchant_postal_code: postal_code
        )

        if station_cords[:latitude].nil? || station_cords[:longitude].nil?
          missing_coords_count += 1
          missing_coords_rows << {
            row: start_row + idx,
            driver: driver_name,
            vehicle_id: line_id,
            vin: vin,
            date: date_str,
            time: time_str,
            gas_station_address: "#{address}, #{city}, #{state} #{postal_code}"
          }
          next
        end        
    
        # 4. Compare distance
        distance = get_distance_between_cords(
          lat1: vehicle_point[:latitude],
          lon1: vehicle_point[:longitude],
          lat2: station_cords[:latitude],
          lon2: station_cords[:longitude]
        )[:body]
    
        # Convert vehicle UTC time to PST
        vehicle_time_pst = Time.parse(vehicle_point[:timeInUtc])
          .in_time_zone("Pacific Time (US & Canada)")
          .strftime("%m/%d/%Y %I:%M:%S %p %Z")

        transaction_time_pst = Time.strptime("#{date_str} #{time_str}", "%m/%d/%y %H:%M:%S")
          .in_time_zone("Pacific Time (US & Canada)")
          .strftime("%m/%d/%Y %I:%M:%S %p %Z")

        if distance > 1000
          # FLAGGED log
          #puts "Row #{start_row + idx} - VIN: #{vin}, Time: #{transaction_time_pst}, Driver: #{driver_name} - FLAGGED (#{distance.round(2)} ft)"
          #puts "Vehicle coordinates at #{vehicle_time_pst}: #{vehicle_point[:latitude]}, #{vehicle_point[:longitude]}"
          #puts "Gas station coordinates: #{station_cords[:latitude]}, #{station_cords[:longitude]}"

          flagged_count += 1
          flagged_rows << {
              row: start_row + idx,
              driver: driver_name,
              vehicle_id: line_id,
              vin: vin,
              date: date_str,
              time: time_str,
              gas_station_address: "#{address}, #{city}, #{state} #{postal_code}",
              distance_between_gas_and_vehicle_ft: distance.round(2)
          }
          results << row_data.merge(flagged: true, distance_between_gas_and_vehicle_ft: distance.round(2))
        else
          # OK log
          #puts "Row #{start_row + idx} - VIN: #{vin}, Time: #{transaction_time_pst}, Driver: #{driver_name} - OK (#{distance.round(2)} ft)"

          passed_count += 1
          passed_rows << {
              row: start_row + idx,
              driver: driver_name,
              vehicle_id: line_id,
              vin: vin,
              date: date_str,
              time: time_str,
              gas_station_address: "#{address}, #{city}, #{state} #{postal_code}",
              distance_between_gas_and_vehicle_ft: distance.round(2)
          }
        end
      rescue Net::OpenTimeout => e
        Rails.logger.warn "Timeout at row #{i}: #{e.message}"
        AnalysisController.progress[:last_row] = start_row + idx
        AnalysisController.progress[:error] = true

        File.write(state_file, {
          last_row: i,
          missing_vehicle_id_rows: missing_vehicle_id_rows,
          missing_coords_rows: missing_coords_rows,
          flagged_rows: flagged_rows,
          passed_rows: passed_rows,
          negative_distance_rows: negative_distance_rows
        }.to_json)
        
  
        if retries < 3
          sleep(2**retries)
          retries += 1
          retry
        else
          # After retries, exit loop gracefully
          Rails.logger.error "Max retries reached for row #{i}. Ending analysis early."
          break
        end
      end
    rescue => e
      safe_vin  = (defined?(vin) && vin) ? vin : "unknown"
      safe_date = (defined?(date_str) && date_str) ? date_str : "?"
      safe_time = (defined?(time_str) && time_str) ? time_str : "?"
      Rails.logger.error "Unexpected error during analysis at row #{start_row + (defined?(idx) ? idx : -1)} (VIN=#{safe_vin}, window=#{safe_date} #{safe_time} local) — #{e.class}: #{e.message}\n#{e.backtrace&.join("\n")}"    
      AnalysisController.progress[:last_row] ||= start_row + idx rescue nil
      AnalysisController.progress[:error] = true
    
    ensure
      # Always write partial/final report
      report_data = {
        missing_vehicle_id_rows: missing_vehicle_id_rows,
        missing_coords_rows: missing_coords_rows,
        flagged_rows: flagged_rows,
        passed_rows: passed_rows,
        negative_distance_rows: negative_distance_rows
      }
  
      start_row_num = start_row
      last_row_num  = AnalysisController.progress[:last_row] || end_row
      
      # Extract month/year from uploaded file name
      original_filename = File.basename(file_path, ".*") # e.g., "07 2025 WEX Billed Transactions"
      month_year_match = original_filename.match(/(\d{2})\s+(\d{4})/) # Matches "07 2025"
      month_year = if month_year_match
                    "#{month_year_match[1]}_#{month_year_match[2]}" # => "07_2025"
                  else
                    Time.now.strftime("%m_%Y") # fallback
                  end

      # Build new filename
      filename = "#{month_year}_DriveCam_Gas_Report_#{start_row_num}_to_#{last_row_num}.xlsx"
      file_path = Rails.root.join("tmp", filename)
      
      File.write(file_path.sub_ext('.json'), report_data.to_json)
      Rails.cache.write("report_path_#{sid}", file_path.sub_ext('.json').to_s, expires_in: 1.hour)
      Rails.cache.write("report_name_#{sid}", filename, expires_in: 1.hour)
  
      Rails.logger.info "Analysis complete (partial or full). Report ready at #{file_path}"
      AnalysisController.stop_flag = false
      File.delete(state_file) if File.exist?(state_file)
    end
  
    # --- Log summary at end ---
    puts "="*80
    puts "ANALYSIS SUMMARY"
    puts "Total rows processed:    #{total_rows}"
    puts "Missing vehicle IDs:     #{missing_vehicle_id_count} (#{((missing_vehicle_id_count.to_f / total_rows) * 100).round(2)}%)"
    puts "Missing coordinates:     #{missing_coords_count} (#{((missing_coords_count.to_f / total_rows) * 100).round(2)}%)"
    puts "Negative odo readings:   #{negative_distance_count} (#{((negative_distance_count.to_f / total_rows) * 100).round(2)}%)"
    puts "Flagged (>1000 ft):      #{flagged_count} (#{((flagged_count.to_f / total_rows) * 100).round(2)}%)"
    puts "Passed (≤1000 ft):       #{passed_count} (#{((passed_count.to_f / total_rows) * 100).round(2)}%)"
    puts "="*80
  
    # Optionally include summary in JSON/HTML output
    summary = {
      total_rows: total_rows,
      missing_vehicle_ids: missing_vehicle_id_count,
      missing_coords: missing_coords_count,
      flagged: flagged_count,
      passed: passed_count
    }
    report_data = {
      missing_vehicle_id_rows: missing_vehicle_id_rows,
      missing_coords_rows: missing_coords_rows,
      flagged_rows: flagged_rows,
      passed_rows: passed_rows,
      negative_distance_rows: negative_distance_rows
    }
  end




  def load_file_data
    @gas_data = session[:gas_data] || []
  end

  def vin_to_id_map
    Rails.cache.fetch("vin_to_id_map", expires_in: 1.hour) do
      url = "https://api.lytx.com/v0/vehicles/all?limit=10000&page=1&includeSubgroups=true"
  
      response = http_get_with_retry(
        url,
        headers: { "Authorization" => "Bearer #{lytx_bearer_token}" },
        purpose: "Lytx vehicles/all"
      )
      
      raise "Vehicle list fetch failed (#{response.status} - #{response.body})" unless response.success?
  
      vehicles = JSON.parse(response.body)["vehicles"] || []
  
      # Normalize VIN to uppercase to handle inconsistent casing
      vehicles.each_with_object({}) do |vehicle, map|
        next unless vehicle["vin"] && vehicle["id"]
        map[vehicle["vin"].to_s.upcase] = vehicle["id"]
      end
    end
  end  

  def get_cords_from_vehicle_time(vehicle_id:, date:, time:, include_sub_groups: false, limit: 10, offset: 0, date_option: "timeInUtc", order: "asc", device_id: nil, group_id: nil)
    raise ArgumentError, "Missing required parameters: date and time" unless date && time
  
    # --- Combine date and time into UTC ISO string ---
    month, day, year = date.split("/")
    full_year = year.length == 2 ? "20#{year}" : year
  
    hour, minute, second = time.split(":")
    hour = hour.rjust(2, "0")

    local_time = Time.use_zone("Pacific Time (US & Canada)") do
      Time.zone.local(full_year.to_i, month.to_i, day.to_i, hour.to_i, minute.to_i, second.to_i)
    end
    
    # Convert to UTC with correct DST handling
    utc_center  = local_time.utc
    utc_start_time = utc_center - 60.seconds
    utc_end_time   = utc_center + 60.seconds
    
  
    result = fetch_vehicle_gps_points(
      vehicle_id: vehicle_id,
      start_time: utc_start_time,
      end_time: utc_end_time,
      include_sub_groups: include_sub_groups,
      limit: limit,
      offset: offset,
      date_option: date_option,
      order: order,
      device_id: device_id,
      group_id: group_id
    )
  
    # If no points found, progressively expand search backward by 1 hour at a time (up to 6 hours)
    hours_back = 1
    while (result[:data].empty? || result.dig(:data, 0, :points).blank?) && hours_back <= 6  
      extended_start_time = (local_time - hours_back.hours).utc
      extended_end_time   = local_time.utc
  
      result = fetch_vehicle_gps_points(
        vehicle_id: vehicle_id,
        start_time: extended_start_time,
        end_time: extended_end_time,
        include_sub_groups: include_sub_groups,
        limit: 200,
        offset: offset,
        date_option: date_option,
        order: "desc", # latest first
        device_id: device_id,
        group_id: group_id
      )
  
      hours_back += 1
    end
  
    # Prioritize latest point found (if any)
    if result[:data].any?
      latest_point = result[:data][0][:points].max_by { |p| Time.parse(p[:timeInUtc]) }
      result[:data][0][:points] = [latest_point] if latest_point
    end
  
    result
  end
  
  
  # Helper to DRY the actual API request + parsing
  def fetch_vehicle_gps_points(vehicle_id:, start_time:, end_time:, include_sub_groups:, limit:, offset:, date_option:, order:, device_id:, group_id:)
    start_iso = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso   = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
  
    params = {
      vehicleIdentifier: vehicle_id,
      includeSubGroups: include_sub_groups,
      limit: limit,
      offset: offset,
      startDate: start_iso,
      endDate: end_iso,
      dateOption: date_option,
      order: order
    }
    params[:deviceIdentifier] = device_id if device_id
    params[:groupIdentifier] = group_id if group_id
  
    query = URI.encode_www_form(params)
    url = "https://data.lytx.com/v1/gps?#{query}"
  
    token = lytx_bearer_token
    raise "Bearer token not available." if token.blank?
    
    headers = { "Authorization" => "Bearer #{token}" }
    
    # Guard against inverted/empty window (can trigger 500s upstream)
    if end_time <= start_time
      end_time = start_time + 120.seconds
      end_iso  = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
      params[:endDate] = end_iso
      query = URI.encode_www_form(params)
      url = "https://data.lytx.com/v1/gps?#{query}"
    end
    
    response = http_get_with_retry(
      url,
      headers: headers,
      max_retries: 5,
      purpose: "Lytx GPS [vehicle_id=#{vehicle_id}] #{start_iso}..#{end_iso}"
    )
    
    body = response.body.to_s
    raise "Request failed with #{response.status}: #{body}" unless response.success?    
    
    return { data: [] } if body.strip.empty?
  
    parsed = JSON.parse(body)
    simplified_data = (parsed["data"] || []).map do |vehicle|
      points = (vehicle["points"] || []).compact
  
      # Log if any point has speed > 0
      points.each do |p|
        if p["speedms"].to_f > 0
          #Rails.logger.warn "Vehicle moving at #{p['speedms']} m/s at #{p['timeInUtc']}"
        end
      end
  
      # Prefer speed == 0; fallback to sorted by time
      prioritized_point = points.find { |p| p["speedms"].to_f == 0 } ||
                          points.sort_by { |p| Time.parse(p["timeInUtc"]) }.first
  
      {
        vehicleId: vehicle["vehicleId"],
        vehicleName: vehicle["vehicleName"],
        points: prioritized_point ? [{
          latitude: prioritized_point["latitude"],
          longitude: prioritized_point["longitude"],
          speedms: prioritized_point["speedms"],
          timeInUtc: prioritized_point["timeInUtc"]
        }] : []
      }
    end
  
    { data: simplified_data }
  end
  
  

  def get_cords_from_address(merchant_address:, merchant_city:, merchant_state:, merchant_postal_code:)
    raise ArgumentError, "Missing required parameters" unless merchant_address && merchant_city && merchant_state && merchant_postal_code
  
    full_address = "#{merchant_address}, #{merchant_city}, #{merchant_state} #{merchant_postal_code}"
    encoded_address = URI.encode_www_form_component(full_address)
  
    api_key = ENV["GOOGLE_API_KEY"]
    url = "https://maps.googleapis.com/maps/api/geocode/json?address=#{encoded_address}&key=#{api_key}"
  
    response = http_get_with_retry(url, purpose: "Google Geocode")
    data = JSON.parse(response.body) rescue {}    
  
    unless data["status"] == "OK"
      Rails.logger.warn "Geocoding failed: #{data['status']} - #{data['error_message'] || 'No details'} (#{url})"
      return { latitude: nil, longitude: nil } # caller will treat as missing coords
    end
    
  
    location = data["results"][0]["geometry"]["location"]
    { latitude: location["lat"], longitude: location["lng"] }
  end
  

  def get_distance_between_cords(lat1:, lon1:, lat2:, lon2:)
    raise ArgumentError, "Missing required parameters" unless lat1 && lon1 && lat2 && lon2
  
    to_rad = ->(deg) { deg.to_f * Math::PI / 180 }
    r = 6371_000 # Earth radius in meters
  
    phi1 = to_rad.call(lat1)
    phi2 = to_rad.call(lat2)
    delta_phi = to_rad.call(lat2) - to_rad.call(lat1)
    delta_lambda = to_rad.call(lon2) - to_rad.call(lon1)
  
    a = Math.sin(delta_phi / 2)**2 +
        Math.cos(phi1) * Math.cos(phi2) * Math.sin(delta_lambda / 2)**2
    c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    distance_meters = r * c
  
    distance_feet = distance_meters * 3.28084
    { body: distance_feet }
  end

  def disable_request_logging
    Rails.logger.silence do
      # Do nothing – this silences both Started/Completed lines
    end
  end
  
end