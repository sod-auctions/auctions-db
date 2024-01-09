package auctions_db

import (
	"context"
	"fmt"
	"github.com/go-pg/pg/v10"
)

type Database struct {
	BatchSize int
	db        *pg.DB
}

type Realm struct {
	tableName struct{} `pg:"realms"`
	Id        int16    `pg:"id,pk"`
	Name      string   `pg:"name"`
}

type AuctionHouse struct {
	tableName struct{} `pg:"auction_houses"`
	Id        int16    `pg:"id,pk"`
	Name      string   `pg:"name"`
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

type CurrentAuction struct {
	tableName      struct{} `pg:"current_auctions"`
	RealmID        int16    `pg:"realm_id,pk"`
	AuctionHouseID int16    `pg:"auction_house_id,pk"`
	ItemID         int      `pg:"item_id,pk"`
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

type currentAuctionsTemp struct {
	tableName      struct{} `pg:"current_auctions_temp"`
	RealmID        int16    `pg:"realm_id,pk"`
	AuctionHouseID int16    `pg:"auction_house_id,pk"`
	ItemID         int      `pg:"item_id,pk"`
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

type CurrentAuctionQueryResult struct {
	RealmID        int16  `pg:"realm_id,pk"`
	AuctionHouseID int16  `pg:"auction_house_id,pk"`
	ItemID         int    `pg:"item_id,pk"`
	ItemName       string `pg:"item_name"`
	ItemMediaURL   string `pg:"item_media_url"`
	ItemRarity     string `pg:"item_rarity"`
	Quantity       int32  `pg:"quantity"`
	Min            int32  `pg:"min,use_zero"`
	Max            int32  `pg:"max,use_zero"`
	P05            int32  `pg:"p05,use_zero"`
	P10            int32  `pg:"p10,use_zero"`
	P25            int32  `pg:"p25,use_zero"`
	P50            int32  `pg:"p50,use_zero"`
	P75            int32  `pg:"p75,use_zero"`
	P90            int32  `pg:"p90,use_zero"`
}

type Item struct {
	tableName     struct{} `pg:"items"`
	Id            int32    `pg:"id,pk"`
	Name          string   `pg:"name"`
	MediaURL      string   `pg:"media_url"`
	Rarity        string   `pg:"rarity"`
	Level         int16    `pg:"level"`
	RequiredLevel int16    `pg:"required_level"`
	PurchasePrice int32    `pg:"purchase_price"`
	SellPrice     int32    `pg:"sell_price"`
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

type PriceAverage struct {
	tableName       struct{} `pg:"price_averages"`
	RealmID         int16    `pg:"realm_id,pk"`
	AuctionHouseId  int16    `pg:"auction_house_id,pk"`
	ItemID          int32    `pg:"item_id,pk"`
	QuantityCurrent int32    `pg:"quantity_current"`
	QuantityAverage int32    `pg:"quantity_average"`
	QuantityPercent float32  `pg:"quantity_percent"`
	P05Current      int32    `pg:"p05_current"`
	P05Average      int32    `pg:"p05_average"`
	p05Percent      float32  `pg:"p05_percent"`
	P10Current      int32    `pg:"p10_current"`
	P10Average      int32    `pg:"p10_average"`
	p10Percent      float32  `pg:"p10_percent"`
	P25Current      int32    `pg:"p25_current"`
	P25Average      int32    `pg:"p25_average"`
	p25Percent      float32  `pg:"p25_percent"`
	P50Current      int32    `pg:"p50_current"`
	P50Average      int32    `pg:"p50_average"`
	p50Percent      float32  `pg:"p50_percent"`
	P75Current      int32    `pg:"p75_current"`
	P75Average      int32    `pg:"p75_average"`
	p75Percent      float32  `pg:"p75_percent"`
	P90Current      int32    `pg:"p90_current"`
	P90Average      int32    `pg:"p90_average"`
	p90Percent      float32  `pg:"p90_percent"`
}

type priceAverageTemp struct {
	tableName       struct{} `pg:"price_averages_temp"`
	RealmID         int16    `pg:"realm_id,pk"`
	AuctionHouseId  int16    `pg:"auction_house_id,pk"`
	ItemID          int32    `pg:"item_id,pk"`
	QuantityCurrent int32    `pg:"quantity_current"`
	QuantityAverage int32    `pg:"quantity_average"`
	QuantityPercent float32  `pg:"quantity_percent"`
	P05Current      int32    `pg:"p05_current"`
	P05Average      int32    `pg:"p05_average"`
	p05Percent      float32  `pg:"p05_percent"`
	P10Current      int32    `pg:"p10_current"`
	P10Average      int32    `pg:"p10_average"`
	p10Percent      float32  `pg:"p10_percent"`
	P25Current      int32    `pg:"p25_current"`
	P25Average      int32    `pg:"p25_average"`
	p25Percent      float32  `pg:"p25_percent"`
	P50Current      int32    `pg:"p50_current"`
	P50Average      int32    `pg:"p50_average"`
	p50Percent      float32  `pg:"p50_percent"`
	P75Current      int32    `pg:"p75_current"`
	P75Average      int32    `pg:"p75_average"`
	p75Percent      float32  `pg:"p75_percent"`
	P90Current      int32    `pg:"p90_current"`
	P90Average      int32    `pg:"p90_average"`
	p90Percent      float32  `pg:"p90_percent"`
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

func (database *Database) GetRealms() ([]Realm, error) {
	var realms []Realm
	_, err := database.db.Query(&realms, "SELECT id,name FROM realms")
	if err != nil {
		return nil, err
	}
	return realms, nil
}

func (database *Database) GetAuctionHouses() ([]AuctionHouse, error) {
	var auctionHouses []AuctionHouse
	_, err := database.db.Query(&auctionHouses, "SELECT id,name FROM auction_houses")
	if err != nil {
		return nil, err
	}
	return auctionHouses, nil
}

func (database *Database) GetItem(itemId int32) (*Item, error) {
	item := &Item{}
	err := database.db.Model(item).Where("id = ?", itemId).Select()
	if err != nil {
		return nil, err
	}
	return item, nil
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

func (database *Database) UpsertItem(item *Item) error {
	_, err := database.db.Model(item).
		OnConflict("(id) DO UPDATE").
		Insert()
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

func (database *Database) GetCurrentAuctions(realmId int16, auctionHouseId int16, orderBy string, direction string, offset int32, limit int16) ([]CurrentAuctionQueryResult, error) {
	var orderByQuery string
	if orderBy == "p50" {
		orderByQuery = "p50"
	} else {
		orderByQuery = "quantity"
	}

	var directionQuery string
	if direction == "desc" {
		directionQuery = "DESC"
	} else {
		directionQuery = "ASC"
	}

	query := fmt.Sprintf(`
		SELECT item_id, items.name AS item_name, items.media_url AS item_media_url, items.rarity AS item_rarity, 
		       quantity, min, max, p05, p10, p25, p50, p75, p90
		FROM current_auctions
		INNER JOIN items ON item_id = items.id
		WHERE realm_id = ? AND auction_house_id = ?
		ORDER BY %s %s
		OFFSET ? LIMIT ?
	`, orderByQuery, directionQuery)

	var currentAuctions []CurrentAuctionQueryResult
	_, err := database.db.Query(&currentAuctions, query, realmId, auctionHouseId, offset, limit)
	if err != nil {
		return nil, err
	}

	return currentAuctions, nil
}

func (database *Database) CountCurrentAuctions(realmId int16, auctionHouseId int16) (int, error) {
	count, err := database.db.Model(&CurrentAuction{}).
		Where("realm_id = ? and auction_house_id = ?", realmId, auctionHouseId).
		Count()
	if err != nil {
		return 0, err
	}
	return count, nil
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

func (database *Database) GetPriceAverages(realmId int16, auctionHouseId int16, sortBy string, offset int32, limit int16) ([]PriceAverage, error) {
	var directionQuery string
	if sortBy == "high" {
		directionQuery = "DESC"
	} else {
		directionQuery = "ASC"
	}

	query := fmt.Sprintf(`
		SELECT item_id, quantity_current, quantity_average, quantity_percent, p05_current, p05_average, p05_percent, 
		       p10_current, p10_average, p10_percent, p25_current, p25_average, p25_percent, p50_current, p50_average, 
		       p50_percent, p75_current, p75_average, p75_percent, p90_current, p90_average, p90_percent
		FROM price_averages
		ORDER BY p05_percent %s
		WHERE realm_id = ? AND auction_house_id = ?
		OFFSET ? LIMIT ?
	`, directionQuery)

	var priceAverages []PriceAverage
	_, err := database.db.Query(&priceAverages, query, realmId, auctionHouseId, offset, limit)
	if err != nil {
		return nil, err
	}
	return priceAverages, nil
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

	tx, err := database.db.Begin()
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("ALTER TABLE price_distributions RENAME TO price_distributions_temp2")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("ALTER TABLE price_distributions_temp RENAME TO price_distributions")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("ALTER TABLE price_distributions_temp2 RENAME TO price_distributions_temp")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("TRUNCATE TABLE price_distributions_temp")
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}

	return nil
}

func (database *Database) ReplaceCurrentAuctions(auctions []*Auction) error {
	currentAuctions := make([]*currentAuctionsTemp, len(auctions))
	for i, v := range auctions {
		currentAuctions[i] = &currentAuctionsTemp{
			RealmID:        v.RealmID,
			AuctionHouseID: v.AuctionHouseID,
			ItemID:         v.ItemID,
			Quantity:       v.Quantity,
			Min:            v.Min,
			Max:            v.Max,
			P05:            v.P05,
			P10:            v.P10,
			P25:            v.P25,
			P50:            v.P50,
			P75:            v.P75,
			P90:            v.P90,
		}
	}

	for i := 0; i < len(currentAuctions); i += database.BatchSize {
		end := i + database.BatchSize
		if end > len(currentAuctions) {
			end = len(currentAuctions)
		}
		batch := currentAuctions[i:end]
		_, err := database.db.Model(&batch).Insert()
		if err != nil {
			return err
		}
	}

	tx, err := database.db.Begin()
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("ALTER TABLE current_auctions RENAME TO current_auctions_temp2")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("ALTER TABLE current_auctions_temp RENAME TO current_auctions")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("ALTER TABLE current_auctions_temp2 RENAME TO current_auctions_temp")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("TRUNCATE TABLE current_auctions_temp")
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}

	return nil
}

func (database *Database) ReplacePriceAverages(priceAverages []*PriceAverage) error {
	priceAveragesTemp := make([]*priceAverageTemp, len(priceAverages))
	for i, v := range priceAverages {
		priceAveragesTemp[i] = &priceAverageTemp{
			RealmID:         v.RealmID,
			AuctionHouseId:  v.AuctionHouseId,
			ItemID:          v.ItemID,
			QuantityCurrent: v.QuantityCurrent,
			QuantityAverage: v.QuantityAverage,
			QuantityPercent: v.QuantityPercent,
			P05Current:      v.P05Current,
			P05Average:      v.P05Average,
			p05Percent:      v.p05Percent,
			P10Current:      v.P10Current,
			P10Average:      v.P10Average,
			p10Percent:      v.p10Percent,
			P25Current:      v.P25Current,
			P25Average:      v.P25Average,
			p25Percent:      v.p25Percent,
			P50Current:      v.P50Current,
			P50Average:      v.P50Average,
			p50Percent:      v.p50Percent,
			P75Current:      v.P75Current,
			P75Average:      v.P75Average,
			p75Percent:      v.p75Percent,
			P90Current:      v.P90Current,
			P90Average:      v.P90Average,
			p90Percent:      v.p90Percent,
		}
	}

	for i := 0; i < len(priceAveragesTemp); i += database.BatchSize {
		end := i + database.BatchSize
		if end > len(priceAverages) {
			end = len(priceAverages)
		}
		batch := priceAveragesTemp[i:end]
		_, err := database.db.Model(&batch).Insert()
		if err != nil {
			return err
		}
	}

	tx, err := database.db.Begin()
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("ALTER TABLE price_averages RENAME TO price_averages_temp2")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("ALTER TABLE price_averages_temp RENAME TO price_averages")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("ALTER TABLE price_averages_temp2 RENAME TO price_averages_temp")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("TRUNCATE TABLE price_averages_temp")
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}

	return nil
}
