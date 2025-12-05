tests:
	@. ./.test.env && go clean -testcache && go test -cover -race ./...

coverage:
	@. ./.test.env && go test -coverprofile=coverage.out ./...


%::
	@true