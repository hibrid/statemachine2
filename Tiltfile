load('Tiltfile.global', 'getAbsoluteDir', 'getNested', 'getConfig', 'getHelmValuesFile', 'getHelmOverridesFile', 'isShutdown')

allow_k8s_contexts('kind-admin@mk')

### Config Start ###
statemachine_helm_values_file = getHelmValuesFile()
statemachine_helm_overrides_file = getHelmOverridesFile()
config = getConfig()

watch_file(statemachine_helm_values_file)
watch_file(statemachine_helm_overrides_file)

statemachine2_helm_chart_dir = "./deployments/examples"

is_shutdown = isShutdown()
### Config End ###



### Main Start ###
def main():

  # Set up tidepool helm template command
  statemachine_helm_template = 'helm template --namespace default '

  if not is_shutdown:
    updateHelmDependancies()
    provisionClusterRoleBindings()
    provisionServerSecrets()
    provisionConfigMaps()

    # Ensure kafka service is deployed
    kafka_service = local('kubectl get service kafka-kafka-bootstrap --ignore-not-found')
    if not kafka_service:
      local('tilt up --file=Tiltfile.kafka --legacy=0 --port=0 >/dev/null 2>&1 &')

     # Ensure proxy services are deployed
    gateway_proxy_service = local('kubectl get service gateway-proxy --ignore-not-found -n default')
    if not gateway_proxy_service:
      fail("Gateway service is missing. Please install gateway via glooctl")

    # Wait until kafka is ready and kafka secrets are created
    if not kafka_service:
      print("Preparing kafka service...")
      local('while [ -z "$(kubectl get secret kafka --ignore-not-found)" ]; do sleep 5; done')
      print("Kafka ready.")

  else:

    local('SHUTTING_DOWN=1 tilt down --file=Tiltfile.gateway &>/dev/null &')

    local('SHUTTING_DOWN=1 tilt down --file=Tiltfile.kafka &>/dev/null &')

    # Clean up any tilt up background processes
    local('for pid in $(ps -o pid,args | awk \'$2 ~ /tilt/ && $3 ~ /up/ {print $1}\'); do kill -9 $pid; done')

  # Apply any service overrides
  statemachine_helm_template += '-f {baseConfig} -f {overrides} '.format(
    baseConfig=statemachine_helm_values_file,
    overrides=statemachine_helm_overrides_file,
  )
  statemachine_helm_template = applyServiceOverrides(statemachine_helm_template)
  
  # Don't provision the gloo gateway here - we do that in Tiltfile.gateway
  statemachine_helm_template += '--set "gloo.enabled=false" --set "gloo.created=true" '

  # Set release name
  statemachine_helm_template += '--name-template "st" '

  # Deploy and watch the helm charts
  k8s_yaml(
    [
      local('{helmCmd} {chartDir}'.format(
      chartDir=statemachine2_helm_chart_dir,
      helmCmd=statemachine_helm_template)),
    ]
  )

  # To update on helm chart source changes, uncomment below
  # watch_file(tidepool_helm_chart_dir)

  # Back out of actual provisioning for debugging purposes by uncommenting below
  # fail('NOT YET ;)')
### Main End ###

### Helm Dependancies Update Start ###
def updateHelmDependancies():
  local('cd deployments/examples && for dep in $(helm dep list | grep "file://" | cut -f 3 | sed s#file:/#.#); do helm dep update $dep; done')
  local('cd deployments/examples && helm dep up')
### Helm Dependancies Update End ###

### Cluster Role Bindings Start ###
def provisionClusterRoleBindings():
  required_admin_clusterrolebindings = [
    'default',
  ]

  for serviceaccount in required_admin_clusterrolebindings:
    clusterrolebinding = local('kubectl get clusterrolebinding {serviceaccount}-admin --ignore-not-found'.format(
      serviceaccount = serviceaccount
    ))

    if not clusterrolebinding:
      local('kubectl create clusterrolebinding {serviceaccount}-admin --clusterrole cluster-admin --serviceaccount=default:{serviceaccount} --validate=0'.format(
        serviceaccount = serviceaccount
      ))
### Cluster Role Bindings End ###

### Secrets Start ###
def provisionServerSecrets ():
    required_secrets = [
    ]

    secretHelmKeyMap = {
        'kissmetrics': 'global.secret.templated',
    }

    secretChartPathMap = {
        'kissmetrics': 'highwater/charts/kissmetrics/templates/kissmetrics-secret.yaml',
    }

  # Skip secrets already available on cluster
    existing_secrets = str(local("kubectl get secrets -o=jsonpath='{.items[?(@.type==\"Opaque\")].metadata.name}'")).split()
    for existing_secret in existing_secrets:
        if existing_secret in required_secrets:
            required_secrets.remove(existing_secret)

    for secret in required_secrets:
        secretChartPath = secretChartPathMap.get(secret, '{secret}/templates/0-secret.yaml'.format(
            secret=secret,
        ))

        templatePath = 'deployments/examples/charts/{secretChartPath}'.format(
            secretChartPath=secretChartPath,
        )

        secretKey = secretHelmKeyMap.get(secret, '{}.secret.enabled'.format(secret))

        # Generate the secret and apply it to the cluster
        local('helm template {chartDir} --namespace default --set "{secretKey}=true" -s {templatePath} -f {baseConfig} -f {overrides} -g | kubectl --namespace=default apply --validate=0 --force -f -'.format(
            chartDir=getAbsoluteDir(statemachine2_helm_chart_dir),
            templatePath=templatePath,
            secretKey=secretKey,
            baseConfig=statemachine_helm_values_file,
            overrides=statemachine_helm_overrides_file,
        ))
### Secrets End ###

### Config Maps Start ###
def provisionConfigMaps ():
  required_configmaps = [
    #'statemachine2',
  ]

  # Skip configmaps already available on cluster
  existing_configmaps = str(local("kubectl get --ignore-not-found configmaps -o=jsonpath='{.items[].metadata.name}'")).split()
  for existing_configmap in existing_configmaps:
    if ','.join(required_configmaps).find(existing_configmap) >= 0:
      required_configmaps.remove(existing_configmap)

  for configmap in required_configmaps:
    configmapChartPath = '{configmap}/templates/0-configmap.yaml'.format(
      configmap=configmap,
    )

    templatePath = 'deployments/examples/{configmapChartPath}'.format(
      configmapChartPath=configmapChartPath,
    )

    # Generate the configmap and apply it to the cluster
    local('helm template {chartDir} --namespace default -s {templatePath} -f {baseConfig} -f {overrides} -g | kubectl --namespace=default apply --validate=0 --force -f -'.format(
      chartDir=getAbsoluteDir(statemachine2_helm_chart_dir),
      baseConfig=statemachine_helm_values_file,
      overrides=statemachine_helm_overrides_file,
      templatePath=templatePath
    ))
### Config Maps End ###

### Service Overrides Start ###
def applyServiceOverrides(statemachine_helm_template):
  for service, overrides in config.items():
    if type(overrides) == 'dict' and overrides.get('hostPath') and getNested(overrides, 'deployment.image'):
      hostPath = getAbsoluteDir(overrides.get('hostPath'))
      containerPath = overrides.get('containerPath')
      dockerFile = overrides.get('dockerFile', 'Dockerfile')
      target = overrides.get('buildTarget', 'development')

      fallback_commands = []
      sync_commands = []
      run_commands = []
      build_deps = [hostPath]

      buildCommand = 'DOCKER_BUILDKIT=1 docker build --file {dockerFile} -t $EXPECTED_REF'.format(
        dockerFile='{}/{}'.format(hostPath, dockerFile),
        target=target,
      )

      if target:
        buildCommand += ' --target {}'.format(target)

      preBuildCommand = ''
      postBuildCommand = ''

      # Force rebuild when Dockerfile changes
      fallback_commands.append(fall_back_on([
        '{}/{}'.format(hostPath, dockerFile),
      ]))

      # Sync the host path changes to the container path
      sync_commands.append(sync(hostPath, containerPath))

      buildCommand += ' {}'.format(hostPath)

      # Apply any rebuild commands specified
      if overrides.get('rebuildCommand'):
        run_commands.append(run(overrides.get('rebuildCommand')))

      # Apply container process restart if specified
      entrypoint = overrides.get('restartContainerCommand', '');
      if overrides.get('restartContainerCommand'):
        run_commands.append(run('./tilt/restart.sh'))

      live_update_commands = fallback_commands + sync_commands + run_commands

      custom_build(
        ref=getNested(overrides, 'deployment.image'),
        entrypoint=entrypoint,
        command='{} {} {}'.format(preBuildCommand, buildCommand, postBuildCommand),
        deps=build_deps,
        disable_push=False,
        tag='tilt',
        live_update=live_update_commands
      )

  return statemachine_helm_template
### Service Overrides End ###

# Unleash the beast
main()