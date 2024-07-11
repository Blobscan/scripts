import { client } from "../clients/ethereum";
import { prisma } from "../clients/prisma";

try {
  let cursor: { timestamp: Date } | undefined;

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

    const txPromises = [];

    for (const dbBlock of dbBlockWithTxs) {
      txPromises.push(
        client
          .getBlock({
            blockNumber: BigInt(dbBlock.number),
          })
          .then((fullBlock) =>
            dbBlock.transactions.map((tx) => {
              const index = fullBlock.transactions.findIndex(
                (txHash) => txHash === tx.hash
              );

              if (index === -1) {
                throw new Error(
                  `Transaction ${tx.hash} not found in block ${dbBlock.number}`
                );
              }

              return {
                hash: tx.hash,
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

    console.log(
      `${dbBlockWithTxs[0]?.number} - ${
        dbBlockWithTxs[dbBlockWithTxs.length - 1]?.number
      }: Indexes updated for ${txsWithIndexes.length} txs in ${
        dbBlockWithTxs.length
      } blocks. Total: ${totalT1 - totalT0}ms; Blocks fetching: ${
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
