# -*- mode: ruby -*-
# vi: set ft=ruby :

# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure(2) do |config|
  # The most common configuration options are documented and commented below.
  # For a complete reference, please see the online documentation at
  # https://docs.vagrantup.com.

  # Every Vagrant development environment requires a box. You can search for
  # boxes at https://atlas.hashicorp.com/search.
  config.vm.box = "centos65-x86_64-20140116"

  # Disable automatic box update checking. If you disable this, then
  # boxes will only be checked for updates when the user runs
  # `vagrant box outdated`. This is not recommended.
  # config.vm.box_check_update = false

  # Create a forwarded port mapping which allows access to a specific port
  # within the machine from a port on the host machine. In the example below,
  # accessing "localhost:8080" will access port 80 on the guest machine.
  # config.vm.network "forwarded_port", guest: 80, host: 8080

  #hbase
  #config.vm.network "forwarded_port", guest: 60000, host: 60000
  #config.vm.network "forwarded_port", guest: 60010, host: 60010
  #config.vm.network "forwarded_port", guest: 60020, host: 60020
  #config.vm.network "forwarded_port", guest: 60030, host: 60030
  #config.vm.network "forwarded_port", guest: 8080, host: 8094
  #config.vm.network "forwarded_port", guest: 20550, host: 20550
  #config.vm.network "forwarded_port", guest: 8085, host: 8095
  #config.vm.network "forwarded_port", guest: 9090, host: 9090
  #config.vm.network "forwarded_port", guest: 9095, host: 9095 

  #zk nodes
  #config.vm.network "forwarded_port", guest: 2181, host: 2181

  #hadoop hdfs
  #config.vm.network "forwarded_port", guest: 50010, host: 50010
  #config.vm.network "forwarded_port", guest: 1004, host: 1004
  #config.vm.network "forwarded_port", guest: 50075, host: 50075
  #config.vm.network "forwarded_port", guest: 50475, host: 50475
  #config.vm.network "forwarded_port", guest: 1006, host: 1006
  #config.vm.network "forwarded_port", guest: 50020, host: 50020
  #config.vm.network "forwarded_port", guest: 8020, host: 8020
  #config.vm.network "forwarded_port", guest: 8022, host: 8022
  #config.vm.network "forwarded_port", guest: 50070, host: 50070
  #config.vm.network "forwarded_port", guest: 50470, host: 50470

  #hadoop MapReduce (MRv1)
  #config.vm.network "forwarded_port", guest: 8021, host: 8021
  #config.vm.network "forwarded_port", guest: 8023, host: 8023
  #config.vm.network "forwarded_port", guest: 50030, host: 50030

  #hadoop YARN (MRv2)
  #config.vm.network "forwarded_port", guest: 8032, host: 8032
  #config.vm.network "forwarded_port", guest: 8042, host: 8042
  #config.vm.network "forwarded_port", guest: 8044, host: 8044
  #config.vm.network "forwarded_port", guest: 19888, host: 19888
  #config.vm.network "forwarded_port", guest: 19890, host: 19890

  #hadoop flume
  #config.vm.network "forwarded_port", guest: 41414, host: 41414
  

  # Create a private network, which allows host-only access to the machine
  # using a specific IP.
  # config.vm.network "private_network", ip: "192.168.33.10"
  config.vm.network "private_network", ip: "172.16.0.199"

  # Create a public network, which generally matched to bridged network.
  # Bridged networks make the machine appear as another physical device on
  # your network.
  # config.vm.network "public_network"

  # Share an additional folder to the guest VM. The first argument is
  # the path on the host to the actual folder. The second argument is
  # the path on the guest to mount the folder. And the optional third
  # argument is a set of non-required options.
  # config.vm.synced_folder "../data", "/vagrant_data"

  # Provider-specific configuration so you can fine-tune various
  # backing providers for Vagrant. These expose provider-specific options.
  # Example for VirtualBox:
  #
  # config.vm.provider "virtualbox" do |vb|
  #   # Display the VirtualBox GUI when booting the machine
  #   vb.gui = true
  #
  #   # Customize the amount of memory on the VM:
  #   vb.memory = "1024"
  # end
  #
  # View the documentation for the provider you are using for more
  # information on available options.

  # Define a Vagrant Push strategy for pushing to Atlas. Other push strategies
  # such as FTP and Heroku are also available. See the documentation at
  # https://docs.vagrantup.com/v2/push/atlas.html for more information.
  # config.push.define "atlas" do |push|
  #   push.app = "YOUR_ATLAS_USERNAME/YOUR_APPLICATION_NAME"
  # end

  # Enable provisioning with a shell script. Additional provisioners such as
  # Puppet, Chef, Ansible, Salt, and Docker are also available. Please see the
  # documentation for more information about their specific syntax and use.
  # config.vm.provision "shell", inline: <<-SHELL
  #   sudo apt-get update
  #   sudo apt-get install -y apache2
  # SHELL
end
