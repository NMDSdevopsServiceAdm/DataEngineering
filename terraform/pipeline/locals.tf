locals {
  workspace_prefix           = substr(lower(replace(terraform.workspace, "/[^a-zA-Z0-9]+/", "-")), 0, 30)
  is_development_environment = local.workspace_prefix != "main"
  # Maps StepFunction files in step-functions/dynamic using filenames as keys
  step_functions = tomap({
    for fn in fileset("step-functions/dynamic", "*.json") :
    substr(fn, 0, length(fn) - 5) => "step-functions/dynamic/${fn}"
  })
}