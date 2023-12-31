package auctions_db

import (
	"context"
	"github.com/go-pg/pg/v10"
)

type Database struct {
	BatchSize int
	db        *pg.DB
}

type Auction struct {
	tableName      struct{} `pg:"auctions"`
	RealmID        int16    `pg:"realm_id,pk"`
	AuctionHouseID int16    `pg:"auction_house_id,pk"`
	ItemID         int      `pg:"item_id,pk"`
	Interval       int16    `pg:"interval,pk"`
	Timestamp      int32    `pg:"timestamp,pk"`
	Quantity       int32    `pg:"quantity"`
	Min            int32    `pg:"min,use_zero"`
	Max            int32    `pg:"max,use_zero"`
	P05            int32    `pg:"p05,use_zero"`
	P10            int32    `pg:"p10,use_zero"`
	P25            int32    `pg:"p25,use_zero"`
	P50            int32    `pg:"p50,use_zero"`
	P75            int32    `pg:"p75,use_zero"`
	P90            int32    `pg:"p90,use_zero"`
}

type Item struct {
	tableName struct{} `pg:"items"`
	Id        int32    `pg:"id,pk"`
	Name      string   `pg:"name"`
	MediaURL  string   `pg:"media_url"`
	Rarity    string   `pg:"rarity"`
}

type PriceDistribution struct {
	tableName      struct{} `pg:"price_distributions"`
	RealmID        int16    `pg:"realm_id,pk"`
	AuctionHouseID int16    `pg:"auction_house_id,pk"`
	ItemID         int32    `pg:"item_id,pk"`
	BuyoutEach     int32    `pg:"buyout_each,pk,use_zero"`
	Quantity       int32    `pg:"quantity"`
}

type priceDistributionTemp struct {
	tableName      struct{} `pg:"price_distributions_temp"`
	RealmID        int16    `pg:"realm_id,pk"`
	AuctionHouseID int16    `pg:"auction_house_id,pk"`
	ItemID         int32    `pg:"item_id,pk"`
	BuyoutEach     int32    `pg:"buyout_each,pk,use_zero"`
	Quantity       int32    `pg:"quantity"`
}

func NewDatabase(connString string) (*Database, error) {
	options, err := pg.ParseURL(connString)
	if err != nil {
		return nil, err
	}

	db := pg.Connect(options)
	ctx := context.Background()
	if err := db.Ping(ctx); err != nil {
		return nil, err
	}

	return &Database{
		BatchSize: 1000,
		db:        db,
	}, nil
}

func (database *Database) GetItemIDs() (map[int32]struct{}, error) {
	var itemIds []int32
	err := database.db.Model((*Item)(nil)).Column("id").Select(&itemIds)
	if err != nil {
		return nil, err
	}

	itemsMap := make(map[int32]struct{}, len(itemIds))
	for _, id := range itemIds {
		itemsMap[id] = struct{}{}
	}

	return itemsMap, nil
}

func (database *Database) GetSimilarItems(name string, limit int) ([]Item, error) {
	var items []Item
	_, err := database.db.Query(&items, `
		SELECT id,name,media_url,rarity FROM items
			WHERE name % ?
			ORDER BY similarity(name, ?) DESC
			LIMIT ?
	`, name, name, limit)
	if err != nil {
		return nil, err
	}
	return items, nil
}

func (database *Database) InsertItem(item *Item) error {
	_, err := database.db.Model(item).Insert()
	if err != nil {
		return err
	}
	return nil
}

func (database *Database) GetAuctions(interval int16, realmId int16, auctionHouseId int16, itemId int32, limit int16) ([]Auction, error) {
	var auctions []Auction
	_, err := database.db.Query(&auctions, `
		SELECT timestamp, quantity, min, p05, p10, p25, p50, p75, p90, max
		FROM auctions
		WHERE interval = ? AND realm_id = ? AND auction_house_id = ? AND item_id = ?
		ORDER BY timestamp DESC
		LIMIT ?
	`, interval, realmId, auctionHouseId, itemId, limit)
	if err != nil {
		return nil, err
	}
	return auctions, nil
}

func (database *Database) InsertAuctions(auctions []*Auction) error {
	for i := 0; i < len(auctions); i += database.BatchSize {
		end := i + database.BatchSize
		if end > len(auctions) {
			end = len(auctions)
		}
		batch := auctions[i:end]
		_, err := database.db.Model(&batch).Insert()
		if err != nil {
			return err
		}
	}

	return nil
}

func (database *Database) GetPriceDistributions(realmId int16, auctionHouseId int16, itemId int32) ([]PriceDistribution, error) {
	var priceDistributions []PriceDistribution
	_, err := database.db.Query(&priceDistributions, `
		SELECT buyout_each, quantity
		FROM price_distributions
		WHERE realm_id = ? AND auction_house_id = ? AND item_id = ? ORDER BY buyout_each
	`, realmId, auctionHouseId, itemId)
	if err != nil {
		return nil, err
	}
	return priceDistributions, nil
}

func (database *Database) ReplacePriceDistributions(priceDistributions []*PriceDistribution) error {
	priceDistributionsTemp := make([]*priceDistributionTemp, len(priceDistributions))
	for i, v := range priceDistributions {
		priceDistributionsTemp[i] = &priceDistributionTemp{
			RealmID:        v.RealmID,
			AuctionHouseID: v.AuctionHouseID,
			ItemID:         v.ItemID,
			BuyoutEach:     v.BuyoutEach,
			Quantity:       v.Quantity,
		}
	}

	for i := 0; i < len(priceDistributionsTemp); i += database.BatchSize {
		end := i + database.BatchSize
		if end > len(priceDistributions) {
			end = len(priceDistributions)
		}
		batch := priceDistributionsTemp[i:end]
		_, err := database.db.Model(&batch).Insert()
		if err != nil {
			return err
		}
	}

	//tx, err := database.db.Begin()
	//if err != nil {
	//	tx.Rollback()
	//	return err
	//}
	//
	//_, err = tx.Exec("ALTER TABLE price_distributions RENAME TO price_distributions_temp2")
	//if err != nil {
	//	tx.Rollback()
	//	return err
	//}
	//
	//_, err = tx.Exec("ALTER TABLE price_distributions_temp RENAME TO price_distributions")
	//if err != nil {
	//	tx.Rollback()
	//	return err
	//}
	//
	//_, err = tx.Exec("ALTER TABLE price_distributions_temp2 RENAME TO price_distributions_temp")
	//if err != nil {
	//	tx.Rollback()
	//	return err
	//}
	//
	//_, err = tx.Exec("TRUNCATE TABLE price_distributions_temp")
	//
	//err = tx.Commit()
	//if err != nil {
	//	tx.Rollback()
	//	return err
	//}

	return nil
}
