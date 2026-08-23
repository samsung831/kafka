FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY ["kafka.AccountService/kafka.AccountService.csproj", "kafka.AccountService/"]
COPY ["kafka.Shared/kafka.Shared.csproj", "kafka.Shared/"]

RUN dotnet restore \
    "kafka.AccountService/kafka.AccountService.csproj"

COPY . .

WORKDIR "/src/kafka.AccountService"

RUN dotnet publish \
    "kafka.AccountService.csproj" \
    --configuration Release \
    --no-restore \
    --output /app/publish \
    /p:UseAppHost=false


FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final

WORKDIR /app

COPY --from=build /app/publish .

ENV ASPNETCORE_URLS=http://+:8080
ENV DOTNET_RUNNING_IN_CONTAINER=true

EXPOSE 8080

USER $APP_UID

ENTRYPOINT ["dotnet", "kafka.AccountService.dll"]