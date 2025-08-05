Rails.application.routes.draw do


  get "up" => "rails/health#show", as: :rails_health_check

  root "analysis#index"

  get  "/analysis", to: "analysis#index"
  post "/analysis/import", to: "analysis#import"
  post "/analysis/run", to: "analysis#run"
  post "analysis/set_token", to: "analysis#set_token"
  post "/analysis/stop",    to: "analysis#stop"
  get "/analysis/report", to: "analysis#generate_report"
  get '/analysis/progress', to: 'analysis#progress'

end
