tests:
	@. ./.test.env && go clean -testcache && go test -cover -race ./...

coverage:
	@. ./.test.env && go test -coverprofile=coverage.out ./...

scan:
	@echo "🔍 Running SonarQube Scan..."
	@. ./.test.env && sonar-scanner

%::
	@true