defmodule Pulsar.Auth.None do
  @moduledoc false
  def auth_method_name(_auth), do: nil

  def auth_data(_auth), do: ""
end
