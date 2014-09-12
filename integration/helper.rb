require 'cgi'
require 'beaker/dsl/install_utils'
require 'pp'
require 'set'
require 'test/unit/assertions'
require 'json'
require 'inifile'
require 'httparty'

module ClassifierExtensions
  include Test::Unit::Assertions

  CLASSIFIER_PORT = 1261
  CLASSIFIER_SSL_PORT = 1262
  CLASSIFIER_CONFIG_PATH = '/etc/classifier/conf.d/classifier.conf'
  PE_CLASSIFIER_CONFIG_PATH = '/etc/puppetlabs/console-services/conf.d/classifier.conf'
  GitReposDir = Beaker::DSL::InstallUtils::SourcePath

  LeinCommandPrefix = "cd #{GitReposDir}/classifier; LEIN_ROOT=true"

  def self.initialize_test_config(options, os_families)

    base_dir = File.join(File.dirname(__FILE__), '..')

    install_type = get_option_value(options[:classifier_install_type],
      [:git, :package, :pe], "install type", "CLASSIFIER_INSTALL_TYPE", :git)

    install_mode =
        get_option_value(options[:classifier_install_mode],
                         [:install, :upgrade], "install mode",
                         "CLASSIFIER_INSTALL_MODE", :install)

    puppet_sha = get_option_value(options[:puppet_sha], nil,
                                  "install type", "PUPPET_SHA", nil)

    validate_package_version =
        get_option_value(options[:classifier_validate_package_version],
            [:true, :false], "'validate package version'",
            "CLASSIFIER_VALIDATE_PACKAGE_VERSION", :false)

    expected_rpm_version =
        get_option_value(options[:classifier_expected_rpm_version],
            nil, "'expected RPM package version'",
            "CLASSIFIER_EXPECTED_RPM_VERSION", nil)

    expected_deb_version =
        get_option_value(options[:classifier_expected_deb_version],
                         nil, "'expected DEB package version'",
                         "CLASSIFIER_EXPECTED_DEB_VERSION", nil)

    use_proxies =
        get_option_value(options[:classifier_use_proxies],
          [:true, :false], "'use proxies'", "CLASSIFIER_USE_PROXIES", :false)

    purge_after_run =
        get_option_value(options[:classifier_purge_after_run],
          [:true, :false],
          "'purge packages and perform exhaustive cleanup after run'",
          "CLASSIFIER_PURGE_AFTER_RUN", :false)

    package_build_host =
        get_option_value(options[:classifier_package_build_host],
          nil,
          "'hostname for package build output'",
          "CLASSIFIER_PACKAGE_BUILD_HOST",
          "builds.puppetlabs.lan")

    package_repo_host =
        get_option_value(options[:classifier_package_repo_host],
          nil,
          "'hostname for yum/apt repos'",
          "CLASSIFIER_PACKAGE_REPO_HOST",
          "builds.puppetlabs.lan")

    package_repo_url =
        get_option_value(options[:classifier_package_repo_url],
          nil,
          "'base URL for yum/apt repos'",
          "CLASSIFIER_PACKAGE_REPO_URL",
          "http://#{package_repo_host}/classifier")

    classifier_repo_puppet = get_option_value(options[:classifier_repo_puppet],
      nil, "git repo for puppet source installs", "CLASSIFIER_REPO_PUPPET", nil)

    classifier_repo_hiera = get_option_value(options[:classifier_repo_hiera],
      nil, "git repo for hiera source installs", "CLASSIFIER_REPO_HIERA", nil)

    classifier_repo_facter = get_option_value(options[:classifier_repo_facter],
      nil, "git repo for facter source installs", "CLASSIFIER_REPO_FACTER", nil)

    classifier_repo_classifier = get_option_value(options[:classifier_repo_classifier],
      nil, "git repo for classifier source installs", "CLASSIFIER_REPO_CLASSIFIER", nil)

    classifier_git_ref = get_option_value(options[:classifier_git_ref],
      nil, "git revision of classifier to test against", "REF", nil)

    @config = {
      :base_dir => base_dir,
      :acceptance_data_dir => File.join(base_dir, "acceptance", "data"),
      :os_families => os_families,
      :install_type => install_type,
      :install_mode => install_mode,
      :puppet_sha => puppet_sha,
      :validate_package_version => validate_package_version == :true,
      :expected_rpm_version => expected_rpm_version,
      :expected_deb_version => expected_deb_version,
      :use_proxies => use_proxies == :true,
      :purge_after_run => purge_after_run == :true,
      :package_build_host => package_build_host,
      :package_repo_host => package_repo_host,
      :package_repo_url => package_repo_url,
      :repo_puppet => classifier_repo_puppet.to_s,
      :repo_hiera => classifier_repo_hiera.to_s,
      :repo_facter => classifier_repo_facter.to_s,
      :repo_classifier => classifier_repo_classifier.to_s,
      :git_ref => classifier_git_ref,
    }

    pp_config = PP.pp(@config, "")

    Beaker::Log.notify "Classifier Acceptance Configuration:\n\n#{pp_config}\n\n"
  end

  class << self
    attr_reader :config
  end


  def self.get_option_value(value, legal_values, description,
    env_var_name = nil, default_value = nil)

    # precedence is environment variable, option file, default value
    value = ((env_var_name && ENV[env_var_name]) || value || default_value)
    if value
      value = value.to_sym
    end

    unless legal_values.nil? or legal_values.include?(value)
      raise ArgumentError, "Unsupported #{description} '#{value}'"
    end

    value
  end

  # Return the configuration hash initialized at the start with
  # initialize_test_config
  #
  # @return [Hash] configuration hash
  def test_config
    ClassifierExtensions.config
  end

  #This classifier method forces a returned host that has a defined role of
  #classifier in the beaker config. Once beaker PR 362 has been merged, this
  #definition in this file should no longer be necessary.
  def classifier
    find_only_one(:classifier)
  end

  def get_os_family(host)
    on(host, "which yum", :silent => true)
    if result.exit_code == 0
      :redhat
    else
      :debian
    end
  end

  def classifier_prefix(host)
    if host.is_pe?
      ""
    else
      ""
    end
  end

  def classifier_service_name(host)
    if host.is_pe?
      "pe-console-services"
    else
      "classifier"
    end
  end

  def classifier_terminus_name(host)
    if host.is_pe?
      "pe-console-services-termini"
    else
      "classifier-terminus"
    end
  end

  def classifier_confdir(host)
    if host.is_pe?
      "/etc/puppetlabs/console-services/conf.d"
    else
      "/etc/classifier/conf.d"
    end
  end

  def classifier_sharedir(host)
    if host.is_pe?
      "/opt/puppet/share/pe-console-services"
    else
      "/usr/share/classifier"
    end
  end

  def classifier_sbin_dir(host)
    if host.is_pe?
      "/opt/puppet/sbin"
    else
      "/usr/sbin"
    end
  end

  def start_classifier(host)
    step "Starting Classifier" do
      if host.is_pe?
        on host, "service pe-console-services stop"
        on host, "service pe-console-services start"
        sleep_until_started_ssl(host)
      else
        on host, "service classifier stop"
        on host, "service classifier start"
        sleep_until_started(host)
      end
    end
  end

  def sleep_until_started(host)
    prefix = classifier_prefix(host)
    curl_with_retries("start classifier", host, "http://localhost:#{CLASSIFIER_PORT}#{prefix}/v1/environments", 0, 120)
  end

  def sleep_until_started_ssl(host)
    prefix = classifier_prefix(host)
    curl_with_retries_ssl("start classifier", host, "https://localhost:#{CLASSIFIER_SSL_PORT}#{prefix}/v1/environments", 0, 120)
  end

  def get_package_version(host, version = nil)
    return version unless version.nil?

    ## These 'platform' values come from the acceptance config files, so
    ## we're relying entirely on naming conventions here.  Would be nicer
    ## to do this using lsb_release or something, but...
    if host['platform'].include?('el-5')
      "#{ClassifierExtensions.config[:expected_rpm_version]}.el5"
    elsif host['platform'].include?('el-6')
      "#{ClassifierExtensions.config[:expected_rpm_version]}.el6"
    elsif host['platform'].include?('ubuntu') or host['platform'].include?('debian')
      "#{ClassifierExtensions.config[:expected_deb_version]}"
    else
      raise ArgumentError, "Unsupported platform: '#{host['platform']}'"
    end

  end

  def install_classifier(host)
    os = test_config[:os_families][host.name]

    case os
    when :debian
      on host, "apt-get install -y --force-yes #{classifier_service_name(classifier)}"
    when :redhat
      on host, "yum install -y #{classifier_service_name(classifier)}"
    else
      raise ArgumentError, "Unsupported OS '#{os}'"
    end
  end

  def validate_package_version(host)
    step "Verifying package version" do
      os = ClassifierExtensions.config[:os_families][host.name]
      installed_version =
        case os
          when :debian
            result = on host, "dpkg-query --showformat \"\\${Version}\" --show classifier"
            result.stdout.strip
          when :redhat
            result = on host, "rpm -q classifier --queryformat \"%{VERSION}-%{RELEASE}\""
            result.stdout.strip
          else
            raise ArgumentError, "Unsupported OS family: '#{os}'"
        end
      expected_version = get_package_version(host)

      Beaker::Log.notify "Expecting package version: '#{expected_version}', actual version: '#{installed_version}'"
      if installed_version != expected_version
        raise RuntimeError, "Installed version '#{installed_version}' did not match expected version '#{expected_version}'"
      end
    end
  end

  def install_classifier_terminus(host, database)
    os = test_config[:os_families][host.name]

    case os
    when :debian
      on host, "apt-get install -y --force-yes #{classifier_terminus_name(master)}"
    when :redhat
      on host, "yum install -y #{classifier_terminus_name(master)}"
    else
      raise ArgumentError, "Unsupported OS '#{os}'"
    end

    install_terminus_config(host, database, classifier_prefix(host))
  end

  def install_terminus_config(host, database, prefix = '')
    create_remote_file(host, "#{host['puppetpath']}/classifier.yaml",
                      "---\n" +
                      "server: #{database}\n" +
                      "port: #{CLASSIFIER_SSL_PORT}\n" +
                      "prefix: \"#{prefix}\"")
    on host, "chmod 644 #{host['puppetpath']}/classifier.yaml"
  end

  def remove_terminus_config(host)
    on host, "rm #{host['puppetpath']}/classifier.yaml"
  end

  def print_ini_files(host)
    step "Print out jetty.ini for posterity" do
      on host, "cat /etc/classifier/conf.d/jetty.ini"
    end
    step "Print out database.ini for posterity" do
      on host, "cat /etc/classifier/conf.d/database.ini"
    end
  end

  def is_gem_installed_on?(host, gem)
    # Include a trailing space when grep'ing to force an exact match of the gem name,
    # so, for example, when checking for 'rspec' we don't match with 'rspec-core'.
    result = on host, "gem list #{gem} | grep \"#{gem} \"", :acceptable_exit_codes => [0,1]
    result.exit_code == 0
  end

  def current_time_on(host)
    result = on host, %Q|date --rfc-2822|
    CGI.escape(Time.rfc2822(result.stdout).iso8601)
  end

  ############################################################################
  # NOTE: the following methods should only be called during run-from-source
  #  acceptance test runs.
  ############################################################################

  def create_pe_databases_on(host, db_specs)
    manifest = ''
    manifest << <<-EOS
    class { '::postgresql::globals':
      user                => 'pe-postgres',
      group              => 'pe-postgres',
      client_package_name => 'pe-postgresql',
      server_package_name => 'pe-postgresql-server',
      service_name        => 'pe-postgresql',
      default_database    => 'pe-postgres',
      version            => '9.2',
      bindir              => '/opt/puppet/bin',
      datadir            => '/opt/puppet/var/lib/pgsql/9.2/data',
      confdir            => '/opt/puppet/var/lib/pgsql/9.2/data',
      psql_path          => '/opt/puppet/bin/psql',
      pg_hba_conf_path => '/opt/puppet/var/lib/pgsql/9.2/data/pg_hba.conf'
    }


    # get the pg server up and running
    class { '::postgresql::server':
      listen_addresses        => '#{host}',
      ip_mask_allow_all_users => '0.0.0.0/0',
      require                => Class['::postgresql::globals'],
    }
    EOS

    db_specs.each do |spec|
      manifest << <<-EOS % spec
      postgresql::server::db{ "%{database}":
        user      => '%{user}',
        password  => '%{password}',
        require => Class['::postgresql::server']
      }
      EOS
    end
    apply_manifest_on(host, manifest)
    on host, 'puppet agent -t', :acceptable_exit_codes => [0,2]
  end

  def install_postgres(host)
    Beaker::Log.notify "Installing postgres on #{host}"

    manifest = ''

    os, version, _ = host['platform'].split('-')
    if os == 'el' && version == '7'
      # Use the builtin postgres as postgresql.org has no repo for rhel7
      # We set all the variables manually because the puppet module
      # doesn't support rhel7 and will get them wrong otherwise
      manifest << <<-EOS
      class { 'postgresql::globals':
        version => '9.2',
        client_package_name => 'postgresql',
        server_package_name => 'postgresql-server',
        contrib_package_name => 'postgresql-contrib',
        java_package_name => 'postgresql-jdbc',
        service_name => 'postgresql',
        bindir => '/usr/bin',
        datadir => '/var/lib/pgsql/data',
      } ->
      EOS
    else
      manifest << <<-EOS
      class { 'postgresql::globals':
        manage_package_repo => true,
        version => '9.2',
      } ->
      EOS
    end

    manifest << <<-EOS
    class { 'postgresql::server': }

    postgresql::server::db { '#{classifier_service_name(host)}':
      user => '#{classifier_service_name(host)}',
      password => 'classifier',
      require => Class['::postgresql::server'],
    }

    postgresql::server::db { 'perbac':
      user => 'perbac',
      password => 'perbac',
      require => Class['::postgresql::server'],
    }
    EOS
    apply_manifest_on(host, manifest)
  end

  def install_classifier_from_source(host)
    # on host, "rm -rf /etc/classifier/ssl"
    manifest = <<-EOS
    group { 'classifier':
      ensure => present,
      system => true,
    }
    user { 'classifier':
      ensure => present,
      gid => 'classifier',
      managehome => false,
      system => true,
    }
    EOS
    apply_manifest_on(host, manifest)

    on host, "#{LeinCommandPrefix} prefix=/usr make -e install-classifier"
  end

  def install_classifier_terminus_from_source(host, database)
    on host, "#{LeinCommandPrefix} make install-terminus"
    install_terminus_config(host, database)
  end

  ###########################################################################

  def get_classifier_configuration(host)
    conf_path = host.is_pe? ? PE_CLASSIFIER_CONFIG_PATH : CLASSIFIER_CONFIG_PATH
    if host.file_exist?(conf_path)
      raw = on(host, "cat #{conf_path}").stdout.strip
      JSON.parse(raw)
    else
      {}
    end
  end

  def set_classifier_configuration(host, conf)
    conf_path = host.is_pe? ? PE_CLASSIFIER_CONFIG_PATH : CLASSIFIER_CONFIG_PATH
    create_remote_file(host, conf_path, JSON.pretty_generate(conf))
    on(host, "chmod 644 #{conf_path}")
  end

  def write_conf_file(host, file, conf)
    conf_path = File.join(classifier_confdir(host), file)
    create_remote_file(host, conf_path, JSON.pretty_generate(conf))
    on(host, "chmod 644 #{conf_path}")
  end

  def stop_classifier(host)
    if host.is_pe?
      on host, "service pe-console-services stop"
      sleep_until_stopped_ssl(host)
    else
      on host, "service classifier stop"
      sleep_until_stopped(host)
    end
  end

  def sleep_until_stopped(host)
    curl_with_retries("stop classifier", host, "http://localhost:#{CLASSIFIER_PORT}", 7)
  end

  def sleep_until_stopped_ssl(host)
    curl_with_retries_ssl("stop classifier", host, "http://localhost:#{CLASSIFIER_PORT}", 7)
  end

  def restart_classifier(host)
    stop_classifier(host)
    start_classifier(host)
  end

  def clear_and_restart_classifier(host)
    stop_classifier(host)
    clear_database(host)
    start_classifier(host)
  end

  def apply_manifest_on(host, manifest_content)
    manifest_path = host.tmpfile("classifier_manifest.pp")
    create_remote_file(host, manifest_path, manifest_content)
    Beaker::Log.notify "Applying manifest on #{host}:\n\n#{manifest_content}"
    on host, puppet_apply("--detailed-exitcodes #{manifest_path}"), :acceptable_exit_codes => [0,2]
  end

  def curl_with_retries_ssl(desc, host, url, desired_exit_codes, max_retries = 60, retry_interval = 1)
    curl_command = "curl --cacert `puppet agent --configprint localcacert` --cert `puppet agent --configprint hostcert` --key `puppet agent --configprint hostprivkey` --insecure -f "
    desired_exit_codes = [desired_exit_codes].flatten
    on(host, curl_command + url, :acceptable_exit_codes => (0...127))
    num_retries = 0
    until desired_exit_codes.include?(exit_code)
      sleep retry_interval
      on(host, curl_command + url, :acceptable_exit_codes => (0...127))
      num_retries += 1
      if (num_retries > max_retries)
        fail("Unable to #{desc}")
      end
    end
  end

  def clear_database(host)
    if host.is_pe? && classifier == master
      on host, 'su - pe-postgres -s "/bin/bash" -c "/opt/puppet/bin/dropdb pe-classifier"'
      on host, 'su - pe-postgres -s "/bin/bash" -c "/opt/puppet/bin/createdb pe-classifier pe-classifier"'
    else
      on host, "su postgres -c 'dropdb #{classifier_service_name(host)}'"
      install_postgres(host)
    end
  end

  #########################################################
  # Classifier export utility functions
  #########################################################
  # These are for comparing classifier export tarballs.
  # This seems like a pretty ridiculous place to define them,
  # but there are no other obvious choices that I see at the
  # moment.  Should consider moving them to a ruby utility
  # code folder in the main Classifier source tree if such a
  # thing ever materializes.

  # @param export_file1 [String] path to first export file
  # @param export_file2 [String] path to second export file
  # @param opts [Hash] comparison options
  # @option opts [Boolean] :catalogs compare catalog? defaults to true
  # @option opts [Boolean] :metadata compare metadata? defaults to true
  # @option opts [Boolean] :reports compare reports? defaults to true
  def compare_export_data(export_file1, export_file2, opts={})
    # Apply defaults
    opts = {
      :catalogs => true,
      :metadata => true,
      :reports => true,
    }.merge(opts)

    # NOTE: I'm putting this tmpdir inside of cwd because I expect for that to
    #  be inside of the jenkins workspace, which I'm hoping means that it will
    #  be cleaned up regularly if we accidentally leave anything lying around
    tmpdir = "./classifier_export_test_tmp"
    FileUtils.rm_rf(tmpdir)
    export_dir1 = File.join(tmpdir, "export1", File.basename(export_file1, ".tar.gz"))
    export_dir2 = File.join(tmpdir, "export2", File.basename(export_file2, ".tar.gz"))
    FileUtils.mkdir_p(export_dir1)
    FileUtils.mkdir_p(export_dir2)

    `tar zxvf #{export_file1} -C #{export_dir1}`
    `tar zxvf #{export_file2} -C #{export_dir2}`

    export1_files = Set.new()
    Dir.glob("#{export_dir1}/**/*") do |f|
      relative_path = f.sub(/^#{export_dir1}\//, "")
      export1_files.add(relative_path)
      expected_path = File.join(export_dir2, relative_path)
      assert(File.exists?(expected_path), "Export file '#{export_file2}' is missing entry '#{relative_path}'")
      puts "Comparing file '#{relative_path}'"
      next if File.directory?(f)
      export_entry_type = get_export_entry_type(relative_path)
      case export_entry_type
        when :catalog
          compare_catalog(f, expected_path) if opts[:catalogs]
        when :metadata
          compare_metadata(f, expected_path) if opts[:metadata]
        when :report
          compare_report(f, expected_path) if opts[:reports]
        when :unknown
          fail("Unrecognized file found in archive: '#{relative_path}'")
      end
    end

    export2_files = Set.new(
      Dir.glob("#{export_dir2}/**/*").map { |f| f.sub(/^#{Regexp.escape(export_dir2)}\//, "") })
    diff = export2_files - export1_files

    assert(diff.empty?, "Export file '#{export_file2}' contains extra file entries: '#{diff.to_a.join("', '")}'")

    FileUtils.rm_rf(tmpdir)
  end

  def get_export_entry_type(path)
    case path
      when "classifier-bak/export-metadata.json"
        :metadata
      when /^classifier-bak\/catalogs\/.*\.json$/
        :catalog
      when /^classifier-bak\/reports\/.*\.json$/
        :report
      else
        :unknown
    end
  end


  def compare_catalog(cat1_path, cat2_path)
    cat1 = munge_catalog_for_comparison(cat1_path)
    cat2 = munge_catalog_for_comparison(cat2_path)

    diff = hash_diff(cat1, cat2)
    if (diff)
      diff = JSON.pretty_generate(diff)
    end

    assert(diff == nil, "Catalogs '#{cat1_path}' and '#{cat2_path}' don't match!' Diff:\n#{diff}")
  end

  def compare_report(cat1_path, cat2_path)
    cat1 = munge_report_for_comparison(cat1_path)
    cat2 = munge_report_for_comparison(cat2_path)

    diff = hash_diff(cat1, cat2)
    if (diff)
      diff = JSON.pretty_generate(diff)
    end

    assert(diff == nil, "Reports '#{cat1_path}' and '#{cat2_path}' don't match!' Diff:\n#{diff}")
  end

  def compare_metadata(meta1_path, meta2_path)
    meta1 = munge_metadata_for_comparison(meta1_path)
    meta2 = munge_metadata_for_comparison(meta2_path)

    diff = hash_diff(meta1, meta2)

    assert(diff == nil, "Export metadata does not match!  Diff\n#{diff}")
  end

  def munge_metadata_for_comparison(meta_path)
    meta = JSON.parse(File.read(meta_path))
    meta.delete("timestamp")
    meta
  end

  def munge_resource_for_comparison(resource)
    resource['tags'] = Set.new(resource['tags'])
    resource
  end

  def munge_catalog_for_comparison(cat_path)
    meta = JSON.parse(File.read(cat_path))
    munged_resources = meta["data"]["resources"].map { |resource| munge_resource_for_comparison(resource) }
    meta["data"]["resources"] = Set.new(munged_resources)
    meta["data"]["edges"] = Set.new(meta["data"]["edges"])
    meta
  end

  def munge_report_for_comparison(cat_path)
    JSON.parse(File.read(cat_path))
  end


  ##############################################################################
  # Object diff functions
  ##############################################################################
  # This is horrible and really doesn't belong here, but I'm not sure where
  # else to put it.  I need a way to do a recursive diff of a hash (which may
  # contain nested objects whose type can be any of Hash, Array, Set, or a
  # scalar).  The hashes may be absolutely gigantic, so if they don't match,
  # I need a way to be able to show a small enough diff so that the user can
  # actually figure out what's going wrong (rather than dumping out the entire
  # gigantic string).  I searched for gems that could handle this and tried
  # 4 or 5 different things, and couldn't find anything that suited the task,
  # so I had to write my own.  This could use improvement, relocation, or
  # replacement with a gem if we ever find a suitable one.
  #
  # UPDATE: chatted with Justin about this and he suggests creating a special
  # puppetlabs-test-utils repo or similar and have that pulled down via
  # bundler, once the acceptance harness is accessible as a gem.  You know,
  # in "The Future".

  # JSON gem doesn't have native support for Set objects, so we have to
  # add this hack.
  class ::Set
    def to_json(arg)
      to_a.to_json(arg)
    end
  end


  def hash_diff(obj1, obj2)
    result =
      (obj1.keys | obj2.keys).inject({}) do |diff, k|
        if obj1[k] != obj2[k]
          objdiff = object_diff(obj1[k], obj2[k])
          if (objdiff)
            diff[k] = objdiff
          end
        end
        diff
      end
    (result == {}) ? nil : result
  end

  def array_diff(arr1, arr2)
    (0..([arr1.length, arr2.length].max)).inject([]) do |diff, i|
      objdiff = object_diff(arr1[i], arr2[i])
      if (objdiff)
        diff << objdiff
      end
      diff
    end
  end

  def set_diff(set1, set2)
    diff1 = set1 - set2
    diff2 = set2 - set1
    unless (diff1.empty? and diff2.empty?)
      [diff1, diff2]
    end
  end

  def object_diff(obj1, obj2)
    if (obj1.class != obj2.class)
      [obj1, obj2]
    else
      case obj1
        when Hash
          hash_diff(obj1, obj2)
        when Array
          array_diff(obj1, obj2)
        when Set
          set_diff(obj1, obj2)
        else
          (obj1 == obj2) ? nil : [obj1, obj2]
      end
    end
  end

  ##############################################################################
  # End Object diff functions
  ##############################################################################

  def install_puppet_dev_repos(sha)
    hosts.each do |host|
      install_dev_repos_on("puppet", host, sha, "repo_configs")
    end
  end

  def install_puppet_from_package
    os_families = test_config[:os_families]
    hosts.each do |host|
      os = os_families[host.name]

      case os
      when :debian
        on host, "apt-get install -y --force-yes puppet puppetmaster-common"
      when :redhat
        on host, "yum install -y puppet"
      else
        raise ArgumentError, "Unsupported OS '#{os}'"
      end
    end
  end

  def install_puppet_from_source
    os_families = test_config[:os_families]

    extend Beaker::DSL::InstallUtils

    source_path = Beaker::DSL::InstallUtils::SourcePath
    git_uri     = Beaker::DSL::InstallUtils::GitURI
    github_sig  = Beaker::DSL::InstallUtils::GitHubSig

    tmp_repositories = []

    repos = Hash[*test_config.select {|k, v| k =~ /^repo_/ and k != 'repo_classifier' }.flatten].values.compact

    repos.each do |uri|
      raise(ArgumentError, "#{uri} is not recognized.") unless(uri =~ git_uri)
      tmp_repositories << extract_repo_info_from(uri)
    end

    repositories = order_packages(tmp_repositories)

    hosts.each_with_index do |host, index|
      os = os_families[host.name]

      case os
      when :redhat
        on host, "yum install -y git-core ruby rubygem-json"
      when :debian
        on host, "apt-get install -y git ruby libjson-ruby"
      else
        raise "OS #{os} not supported"
      end

      on host, "echo #{github_sig} >> $HOME/.ssh/known_hosts"

      repositories.each do |repository|
        step "Install #{repository[:name]}"
        install_from_git host, source_path, repository
      end

      on host, "getent group puppet || groupadd puppet"
      on host, "getent passwd puppet || useradd puppet -g puppet -G puppet"
      on host, "mkdir -p /var/run/puppet"
      on host, "chown puppet:puppet /var/run/puppet"
    end
  end

  def install_puppet_conf
    hosts.each do |host|
      puppetconf = File.join(host['puppetpath'], 'puppet.conf')

      on host, "mkdir -p #{host['puppetpath']}"

      conf = IniFile.new
      conf['agent'] = {
        'server' => master,
      }
      conf['master'] = {
        'pidfile' => '/var/run/puppet/master.pid',
      }
      create_remote_file host, puppetconf, conf.to_s
    end
  end

  def install_puppet
    case test_config[:install_type]
    when :package
      if test_config[:puppet_sha]
        install_puppet_dev_repos(test_config[:puppet_sha])
      end

      install_puppet_from_package
      install_puppet_conf
    when :git
      if test_config[:repo_puppet] then
        install_puppet_from_source
      else
        raise Exception, "You must specify a puppet repository source when install_type is git"
      end
      install_puppet_conf
    end
  end

  # Taken from puppet acceptance lib
  def fetch(base_url, file_name, dst_dir)
    FileUtils.makedirs(dst_dir)
    src = "#{base_url}/#{file_name}"
    dst = File.join(dst_dir, file_name)
    if File.exists?(dst)
      logger.notify "Already fetched #{dst}"
    else
      logger.notify "Fetching: #{src}"
      logger.notify "  and saving to #{dst}"
      open(src) do |remote|
        File.open(dst, "w") do |file|
          FileUtils.copy_stream(remote, file)
        end
      end
    end
    return dst
  end

  def install_release_repos
    hosts.each do |host|
      install_release_repos_on(host, "repo_configs")
    end
  end

  def install_release_repos_on(host, repo_configs_dir)
    platform = host['platform']
    platform_configs_dir = File.join(repo_configs_dir, platform)

    case platform
      when /^(fedora|el|centos)-(\d+)-(.+)$/
        variant = (($1 == 'centos') ? 'el' : $1)
        version = $2
        arch = $3

        rpm_name = "puppetlabs-release-#{variant}-#{version}.noarch.rpm"
        rpm = fetch(
          "http://yum.puppetlabs.com",
          rpm_name,
          platform_configs_dir
        )

        scp_to host, rpm, '/root'
        on host, "rpm -Uvh --force /root/#{rpm_name}"

      when /^(debian|ubuntu)-([^-]+)-(.+)$/
        variant = $1
        version = $2
        arch = $3

        deb_name = "puppetlabs-release-#{version}.deb"
        deb = fetch(
          "http://apt.puppetlabs.com/",
          deb_name,
          platform_configs_dir
        )

        scp_to host, deb, '/root'
        on host, "dpkg -i --force-all /root/#{deb_name}"

        on host, 'apt-get update'
      else
        host.logger.notify("No repository installation step for #{platform} yet...")
    end
  end

  # Taken from puppet acceptance lib
  # Install development repos
  def install_dev_repos_on(package, host, sha, repo_configs_dir)
    platform = host['platform'] =~ /^(debian|ubuntu)/ ? host['platform'].with_version_codename : host['platform']
    platform_configs_dir = File.join(repo_configs_dir, platform)

    case platform
      when /^(fedora|el|centos)-(\d+)-(.+)$/
        variant = (($1 == 'centos') ? 'el' : $1)
        fedora_prefix = ((variant == 'fedora') ? 'f' : '')
        version = $2
        arch = $3

        #hack for https://tickets.puppetlabs.com/browse/RE-1990
        if host.is_pe?
          pattern = "pl-%s-%s-repos-pe-%s-%s%s-%s.repo"
        else
          pattern = "pl-%s-%s-%s-%s%s-%s.repo"
        end
        repo_filename = pattern % [
          package,
          sha,
          variant,
          fedora_prefix,
          version,
          arch
        ]

        repo = fetch(
          "http://builds.puppetlabs.lan/%s/%s/repo_configs/rpm/" % [package, sha],
          repo_filename,
          platform_configs_dir
        )

        scp_to(host, repo, '/etc/yum.repos.d/')

      when /^(debian|ubuntu)-([^-]+)-(.+)$/
        variant = $1
        version = $2
        arch = $3

        list = fetch(
          "http://builds.puppetlabs.lan/%s/%s/repo_configs/deb/" % [package, sha],
          "pl-%s-%s-%s.list" % [package, sha, version],
          platform_configs_dir
        )

        scp_to host, list, '/etc/apt/sources.list.d'
        on host, 'apt-get update'
      else
        host.logger.notify("No repository installation step for #{platform} yet...")
    end
  end

  class Classifier
    include HTTParty
    debug_output($stdout)
    headers({'Content-Type' => 'application/json'})
  end
end

Beaker::TestCase.send(:include, ClassifierExtensions)