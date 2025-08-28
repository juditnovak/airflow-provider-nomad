client {
  host_volume "config" {
    path      = "/home/devel/share/workspace_airflow/nomad_provider/src/airflow/providers/nomad/templates/"
    read_only = true
  }

  host_volume "dags" {
    path      = "/home/devel/share/workspace_airflow/dags/"
    read_only = true
  }

  # host_volume "logs" {
  #   # path      = "/opt/nomad/logs"
  #   path      = "/home/devel/share/workspace_airflow/nomad/runners/logs"
  #   read_only = false 
  # }

# plugin "bridge" {
#   config {
#     cni_version = "1.6.7"
#   }
#
}

