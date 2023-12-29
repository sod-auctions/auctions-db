package auctions_db

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
)

type Database struct {
	BatchSize               int16
	db                      *sql.DB
	realmUpsertQuery        string
	auctionHouseUpsertQuery string
	selectItemIdsQuery      string
	itemUpsertQuery         string
	auctionUpsertQuery      string
}

func NewDatabase(connString string) (*Database, error) {
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}
	return &Database{
		BatchSize: 250,
		db:        db,
		realmUpsertQuery: `
			INSERT INTO realms (id, name)
			VALUES ($1, $2)
			ON CONFLICT (id)
			DO UPDATE SET name = $2 WHERE realms.id = $1
		`,
		auctionHouseUpsertQuery: `
			INSERT INTO auction_houses (id, name)
			VALUES ($1, $2)
			ON CONFLICT (id)
			DO UPDATE SET name = $2 WHERE auction_houses.id = $1
		`,
		selectItemIdsQuery: "SELECT id FROM items",
		itemUpsertQuery: `
			INSERT INTO items (id, name, media_url, rarity)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (id)
			DO UPDATE SET name = $2, media_url = $3, rarity = $4 WHERE items.id = $1
		`,
		auctionUpsertQuery: `
			INSERT INTO auctions (realm_id, auction_house_id, item_id, interval, timestamp,
			                      quantity, min, max, p05, p10, p25, p50, p75, p90)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) 
			ON CONFLICT (realm_id, auction_house_id, item_id, interval, timestamp) 
			DO UPDATE SET quantity = $6, min = $7, max = $8, p05 = $9, p10 = $10, p25 = $11, p50 = $12, 
			    p75 = $13, p90 = $14
		`,
	}, nil
}

type Auction struct {
	RealmID        int16
	AuctionHouseID int16
	ItemID         int
	Interval       int16
	Timestamp      int32
	Quantity       int32
	Min            int32
	Max            int32
	P05            int32
	P10            int32
	P25            int32
	P50            int32
	P75            int32
	P90            int32
}

func (database *Database) InsertAuction(auction *Auction) error {
	stmt, err := database.db.Prepare(database.auctionUpsertQuery)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(auction.RealmID, auction.AuctionHouseID, auction.ItemID, auction.Interval, auction.Timestamp,
		auction.Quantity, auction.Min, auction.Max, auction.P05, auction.P10, auction.P25, auction.P50,
		auction.P75, auction.P90)
	if err != nil {
		return err
	}

	return nil
}

func (database *Database) InsertAuctions(auctions []*Auction) error {
	for i := 0; i < len(auctions); i += int(database.BatchSize) {
		err := database.insertAuctionsBatch(auctions[i:min(i+int(database.BatchSize), len(auctions))])
		if err != nil {
			return err
		}
	}
	return nil
}

type AuctionHouse struct {
	Id   int16
	Name string
}

func (database *Database) InsertAuctionHouse(auctionHouse *AuctionHouse) error {
	stmt, err := database.db.Prepare(database.auctionHouseUpsertQuery)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(auctionHouse.Id, auctionHouse.Name)
	if err != nil {
		return err
	}

	return nil
}

func (database *Database) insertAuctionHouses(auctionHouses []*AuctionHouse) error {
	for i := 0; i < len(auctionHouses); i += int(database.BatchSize) {
		err := database.insertAuctionHousesBatch(auctionHouses[i:min(i+int(database.BatchSize), len(auctionHouses))])
		if err != nil {
			return err
		}
	}
	return nil
}

type Realm struct {
	Id   int16
	Name string
}

func (database *Database) InsertRealm(realm *Realm) error {
	stmt, err := database.db.Prepare(database.realmUpsertQuery)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(realm.Id, realm.Name)
	if err != nil {
		return err
	}

	return nil
}

func (database *Database) InsertRealms(realms []*Realm) error {
	for i := 0; i < len(realms); i += int(database.BatchSize) {
		err := database.insertRealmsBatch(realms[i:min(i+int(database.BatchSize), len(realms))])
		if err != nil {
			return err
		}
	}
	return nil
}

type Item struct {
	Id       int32
	Name     string
	MediaURL string
	Rarity   string
}

func (database *Database) GetItemIDs() (map[int32]struct{}, error) {
	rows, err := database.db.Query(database.selectItemIdsQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make(map[int32]struct{})
	for rows.Next() {
		var id int32
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		ids[id] = struct{}{}
	}

	return ids, nil
}

func (database *Database) InsertItem(item *Item) error {
	stmt, err := database.db.Prepare(database.itemUpsertQuery)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(item.Id, item.Name, item.MediaURL, item.Rarity)
	if err != nil {
		return err
	}

	return nil
}

func (database *Database) InsertItems(items []*Item) error {
	for i := 0; i < len(items); i += int(database.BatchSize) {
		err := database.insertItemsBatch(items[i:min(i+int(database.BatchSize), len(items))])
		if err != nil {
			return err
		}
	}
	return nil
}

func (database *Database) insertItemsBatch(items []*Item) error {
	if len(items) > int(database.BatchSize) {
		return fmt.Errorf("batch size (%v) exceeded", database.BatchSize)
	}

	tx, err := database.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(database.itemUpsertQuery)
	if err != nil {
		return err
	}

	for _, item := range items {
		_, err = stmt.Exec(item.Id, item.Name, item.MediaURL, item.Rarity)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (database *Database) insertRealmsBatch(realms []*Realm) error {
	if len(realms) > int(database.BatchSize) {
		return fmt.Errorf("batch size (%v) exceeded", database.BatchSize)
	}

	tx, err := database.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(database.realmUpsertQuery)
	if err != nil {
		return err
	}

	for _, realm := range realms {
		_, err = stmt.Exec(realm.Id, realm.Name)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (database *Database) insertAuctionHousesBatch(auctionHouses []*AuctionHouse) error {
	if len(auctionHouses) > int(database.BatchSize) {
		return fmt.Errorf("batch size (%v) exceeded", database.BatchSize)
	}

	tx, err := database.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(database.auctionHouseUpsertQuery)
	if err != nil {
		return err
	}

	for _, auctionHouse := range auctionHouses {
		_, err = stmt.Exec(auctionHouse.Id, auctionHouse.Name)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (database *Database) insertAuctionsBatch(auctions []*Auction) error {
	if len(auctions) > int(database.BatchSize) {
		return fmt.Errorf("batch size (%v) exceeded", database.BatchSize)
	}

	tx, err := database.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(database.auctionUpsertQuery)
	if err != nil {
		return err
	}

	for _, auction := range auctions {
		_, err = stmt.Exec(auction.RealmID, auction.AuctionHouseID, auction.ItemID, auction.Interval,
			auction.Timestamp, auction.Quantity, auction.Min, auction.Max, auction.P05, auction.P10, auction.P25,
			auction.P50, auction.P75, auction.P90)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}
