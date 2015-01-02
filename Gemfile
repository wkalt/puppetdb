source ENV['GEM_SOURCE'] || "https://rubygems.org"

group :development, :test do
  case RUBY_VERSION
  when '1.8.7'
    gem 'beaker', '~> 1.20.1',  :require => false
  else
    gem 'beaker',               :require => false
  end
  gem 'rake'
  gem 'puppetlabs_spec_helper', :require => false
  gem 'beaker-rspec',           :require => false
  gem 'serverspec',             :require => false
  gem 'rspec-puppet', '~> 1.0'
  gem 'puppet-lint',  '~> 1.1'
end

if puppetversion = ENV['PUPPET_GEM_VERSION']
  gem 'puppet', puppetversion, :require => false
else
  gem 'puppet', :require => false
end
