job "docs" {
  type = "batch"

  constraint {
    attribute = "${attr.kernel.name}"
    value     = "linux"
  }

  group "example" {
    count = 2
    task "uptime" {
      driver = "docker"
      config {
        image = "hashicorp/http-echo"
        args = ["-text", "hello world"]
      }
    }
  }
}
