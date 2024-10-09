import { PrismaClient } from "@prisma/client";
import { createPublicClient, http } from "viem";
import { mainnet } from "viem/chains";


const prisma = new PrismaClient()
const client = createPublicClient({
  chain: mainnet,
  transport: http(Bun.env.JSON_RPC_API_URL),
});

try {
  let cursor: { timestamp: Date } | undefined;

  const txsWithoutIndexCount = await prisma.transaction.count({
    where: {
      index: null
    }
  })

  let totalTxsUpdated = 0

  do {
    const totalT0 = performance.now();
    const dbBlockWithTxs = await prisma.block.findMany({
      cursor,
      select: {
        hash: true,
        transactions: {
          select: {
            hash: true,
          },
        },
        number: true,
        timestamp: true,
      },
      take: 50,
      skip: cursor ? 1 : 0,
      where: {
        AND: [
          {
            transactions: {
              some: {
                index: {
                  equals: null,
                },
              },
            },
          },
          {
            transactionForks: {
              none: {
                blockHash: {},
              },
            },
          },
        ],
      },
      orderBy: {
        timestamp: "asc",
      },
    });

    if (!dbBlockWithTxs.length) {
      break
    }

    const txPromises = [];

    for (const dbBlock of dbBlockWithTxs) {
      txPromises.push(
        client
          .getBlock({
            blockNumber: BigInt(dbBlock.number),
          })
          .then((fullBlock) =>
            dbBlock.transactions.map((dbTx) => {
              const index = fullBlock.transactions.findIndex(
                (txHash) => txHash === dbTx.hash
              );

              if (index === -1) {
                throw new Error(
                  `DB Transaction ${dbTx.hash} not found in execution block ${fullBlock.number}`
                );
              }

              return {
                hash: dbTx.hash,
                index,
              };
            })
          )
      );
    }

    const blocksT0 = performance.now();
    const txsWithIndexes = (await Promise.all(txPromises)).flat();
    const blocksT1 = performance.now();

    const updateT0 = performance.now();

    await Promise.all(
      txsWithIndexes.map((tx) =>
        prisma.transaction.update({
          where: {
            hash: tx.hash,
          },
          data: {
            index: tx.index,
          },
        })
      )
    );
    const updateT1 = performance.now();

    const totalT1 = performance.now();

    totalTxsUpdated += txsWithIndexes.length


    const pct = (totalTxsUpdated / txsWithoutIndexCount) * 100

    console.log(
      `Blocks ${dbBlockWithTxs[0]?.number} - ${
        dbBlockWithTxs[dbBlockWithTxs.length - 1]?.number
      } (${pct.toFixed(5)}% Total ${txsWithoutIndexCount}): Indexes updated for ${txsWithIndexes.length} txs in ${
        dbBlockWithTxs.length
      } blocks.
Total time: ${totalT1 - totalT0}ms; Blocks fetching: ${
        blocksT1 - blocksT0
      }ms; Tx indexes update: ${updateT1 - updateT0}ms.`
    );

    const lastBlock = dbBlockWithTxs[dbBlockWithTxs.length - 1];


    cursor = lastBlock ? { timestamp: lastBlock.timestamp } : undefined;
  } while (cursor);

  console.log("All tx indexes updated.");
} finally {
  await prisma.$disconnect()
}
