import { createPublicClient, http } from "viem";
import { mainnet } from "viem/chains";

export const client = createPublicClient({
  chain: mainnet,
  transport: http(Bun.env.JSON_RPC_API_URL),
});