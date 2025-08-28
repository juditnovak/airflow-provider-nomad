client {
  host_volume "config" {
    path      = "/home/runner/work/airflow-provider-nomad/airflow-provider-nomad/tests/system/nomad/config/"
    read_only = true
  }

  host_volume "dags" {
    path      = "/home/runner/work/airflow-provider-nomad/airflow-provider-nomad/tests/system/nomad/dags/"
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

