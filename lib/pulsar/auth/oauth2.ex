defmodule Pulsar.Auth.OAuth2 do
  @moduledoc false
  def auth_method_name(_auth), do: "token"

  def auth_data(auth) do
    client =
      OAuth2.Client.new(
        strategy: OAuth2.Strategy.ClientCredentials,
        client_id: auth[:client_id],
        client_secret: auth[:client_secret],
        site: auth[:site]
      )

    resp = OAuth2.Client.get_token!(client, audience: auth[:audience])
    Jason.decode!(resp.token.access_token)["access_token"]
  end
end
