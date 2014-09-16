source ENV['GEM_SOURCE'] || 'https://rubygems.org'

group :test do
  if ENV['GEM_SOURCE']
    gem 'pe-beaker'
  else
    gem 'beaker'
  end
  gem 'httparty'
  gem 'uuidtools'
end
