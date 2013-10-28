step "Install other dependencies on database" do
  os = test_config[:os_families][database.name]

  case test_config[:install_type]
  when :git
    case os
    when :debian
      on database, "apt-get install -y --force-yes openjdk-6-jre-headless rake"
    when :redhat
      on database, "yum install -y java-1.6.0-openjdk rubygem-rake"
    else
      raise ArgumentError, "Unsupported OS '#{os}'"
    end

    step "Install lein on the classifier server" do
      which_result = on database, "which lein", :acceptable_exit_codes => [0,1]
      needs_lein = which_result.exit_code == 1
      if (needs_lein)
        on database, "curl -k https://raw.github.com/technomancy/leiningen/preview/bin/lein -o /usr/local/bin/lein"
        on database, "chmod +x /usr/local/bin/lein"
        on database, "LEIN_ROOT=true lein"
      end
    end
  end
end

# XXX Do I need gems at all?
step "Install rubygems on master" do
  os = test_config[:os_families][master.name]

  case os
  when :redhat
    on master, "yum install -y rubygems"
  when :debian
    on master, "apt-get install -y rubygems"
  else
    raise ArgumentError, "Unsupported OS '#{os}'"
  end

  # Make sure there isn't a gemrc file, because that could ruin our day.
  on master, "rm -f ~/.gemrc"
end
