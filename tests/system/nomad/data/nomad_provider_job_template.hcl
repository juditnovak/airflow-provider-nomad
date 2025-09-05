job "test_nomad_provider_config_default_job_template_hcl" {
  type = "batch"

  group "testgroup" {

    volume "config" {
      type = "host"
      read_only = true
      source = "config"
    }

    volume "dags" {
      type = "host"
      read_only = true
      source = "dags"
    }

    task "airflow" {
      driver = "docker"
      config {
        image = "novakjudit/af_nomad_test:latest"
      }

      env {
        AIRFLOW_CONFIG = "/opt/airflow/config/airflow.cfg"
        AIRFLOW_HOME = "/opt/airflow/"
        AIRFLOW__LOGGING__BASE_LOG_FOLDER = "/usr/src/runner/logs/"
      }

      volume_mount {
        volume      = "config"
        destination = "/opt/airflow/config"
        propagation_mode = "private"
      }

      volume_mount {
        volume      = "dags"
        destination = "/opt/airflow/dags"
        propagation_mode = "private"
      }

    }
  }
}
