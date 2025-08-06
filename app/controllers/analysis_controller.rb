require 'roo'

class AnalysisController < ApplicationController
  before_action :load_file_data, only: :run
  skip_before_action :verify_authenticity_token, only: [:stop]
  @@progress = { current: 0, total: 0 }
  @@stop_flag = false

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

  def bearer_token
    session[:bearer_token]
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
  
    # Save uploaded file to tmp/
    uploaded_file = params[:file]
    tmp_path = Rails.root.join("tmp", uploaded_file.original_filename)
    File.open(tmp_path, "wb") { |f| f.write(uploaded_file.read) }
  
    # Store only path in session
    session[:gas_file_path] = tmp_path.to_s
  
    redirect_to analysis_path, notice: "File uploaded. Click 'Analyze' to start."
  end

  # Run analysis
  def run
    file_path = session[:gas_file_path]
    unless file_path && File.exist?(file_path)
      redirect_to analysis_path, alert: "No uploaded file found. Please upload again."
      return
    end
  
    # Reset stop flag
    AnalysisController.stop_flag = false
    sid = session.id

    # Start the analysis in a separate thread
    Thread.new do
      begin
        perform_analysis(file_path, params[:row_limit], params[:row_offset], sid)
      rescue => e
        Rails.logger.error("Analysis Stopped: #{e.message}")
        AnalysisController.progress[:last_row] = i rescue nil
        AnalysisController.progress[:error] = true
      ensure
        AnalysisController.stop_flag = false
      end
    end
  
    # Immediately return so UI can poll progress and send stop
    redirect_to analysis_path, notice: "Analysis started. Progress will update automatically."
  end
  
  def set_token
    if params[:bearer_token].present?
      session[:bearer_token] = params[:bearer_token]
      session[:bearer_token_set_at] = Time.current.to_i
      redirect_to analysis_path, notice: "Bearer token saved for this session."
    else
      redirect_to analysis_path, alert: "Please provide a valid token."
    end
  end

  def generate_report
    file_path = Rails.cache.read("report_path_#{session.id}")
    filename  = Rails.cache.read("report_name_#{session.id}")
    return redirect_to analysis_path, alert: "No report found. Please run analysis first." unless file_path && File.exist?(file_path)
  
    # Load data from file
    report_data = JSON.parse(File.read(file_path), symbolize_names: true)
  
    package = Axlsx::Package.new
    wb = package.workbook
  
    add_sheet = ->(title, rows) do
      wb.add_worksheet(name: title) do |sheet|
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
    add_sheet.call("Missing Coordinates", report_data[:missing_coords_rows] || [])
    add_sheet.call("Flagged Transactions", report_data[:flagged_rows] || [])
    add_sheet.call("Passed Transactions", passed_rows)
    add_sheet.call("Sunday Transactions", passed_on_sunday)
  
    send_data package.to_stream.read,
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      filename: filename || "analysis_report.xlsx"

  end
  

  



  private







  def perform_analysis(file_path, row_limit_param, row_offset_param, sid)
    spreadsheet = Roo::Spreadsheet.open(file_path)
    state_file = Rails.root.join("tmp", "analysis_state.json")
    headers = spreadsheet.row(1).map { |h| h.to_s.downcase.gsub(/[^\w]+/, "_").to_sym }
    results = []
    if File.exist?(state_file)
      saved_state = JSON.parse(File.read(state_file), symbolize_names: true)
      last_row = saved_state[:last_row] || 2
      missing_vehicle_id_rows = saved_state[:missing_vehicle_id_rows] || []
      missing_coords_rows = saved_state[:missing_coords_rows] || []
      flagged_rows = saved_state[:flagged_rows] || []
      passed_rows = saved_state[:passed_rows] || []
      start_row = last_row + 1
    
      # Counters restored from arrays
      missing_vehicle_id_count = missing_vehicle_id_rows.length
      missing_coords_count = missing_coords_rows.length
      flagged_count = flagged_rows.length
      passed_count = passed_rows.length
    
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
        puts "Sleeping for 0.3 seconds to throttle API requests..." if idx % 100 == 0
        sleep(0.3) # Throttle to avoid overwhelming the API
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
          passed_rows: passed_rows
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
      Rails.logger.error "Unexpected error during analysis: #{e.message}"
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
  
      response = Faraday.get(url) do |req|
        req.headers["Authorization"] = "Bearer #{bearer_token}"
      end
  
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
  
    pst_time = Time.new(full_year.to_i, month.to_i, day.to_i, hour.to_i, minute.to_i, second.to_i)
  
    # --- First query: 1-minute window ---
    utc_start_time = pst_time + 7.hours - 60.seconds
    utc_end_time   = utc_start_time + 120.seconds
  
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
      extended_start_time = pst_time - hours_back.hours + 7.hours # Convert to UTC
      extended_end_time   = pst_time + 7.hours                     # Transaction time in UTC
  
      result = fetch_vehicle_gps_points(
        vehicle_id: vehicle_id,
        start_time: extended_start_time,
        end_time: extended_end_time,
        include_sub_groups: include_sub_groups,
        limit: 1000,
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
  
    raise "Bearer token not set. Please enter it on the index page." unless bearer_token.present?
  
    response = Faraday.get(url) { |req| req.headers["Authorization"] = "Bearer #{bearer_token}" }
    body = response.body
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
  
    response = Faraday.get(url)
    data = JSON.parse(response.body)
  
    unless data["status"] == "OK"
      raise "Geocoding failed: #{data['status']} - #{data['error_message'] || 'No details'}"
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
